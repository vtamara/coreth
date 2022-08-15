// (c) 2020-2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package evm

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ava-labs/avalanchego/chains/atomic"
	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/database/versiondb"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	syncclient "github.com/ava-labs/coreth/sync/client"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

var (
	_ AtomicBackend = &atomicBackend{}
	_ AtomicState   = &atomicState{}
)

// AtomicBackend abstracts the verification and processing
// of atomic transactions
type AtomicBackend interface {
	// CalculateRootWithTxs calculates the root of the atomic trie that would
	// result from applying [txs] to the atomic trie, starting at the state
	// corresponding to previously verified block [parentHash].
	// The atomic trie used to calculate the root is not pinned in memory.
	CalculateRootWithTxs(blockHeight uint64, parentHash common.Hash, txs []*Tx) (common.Hash, error)

	// InsertTxs returns an AtomicState that can be used to transition the VM's
	// atomic state by applying [txs] to the atomic trie, starting at the state
	// corresponding to previously verified block [parentHash].
	// The modified atomic trie is pinned in memory and it's the caller's
	// responsibility to call either Accept or Reject on the returned AtomicState
	// to commit the changes or abort them and free memory.
	InsertTxs(blockHash common.Hash, blockHeight uint64, parentHash common.Hash, txs []*Tx) (AtomicState, error)

	// Returns an AtomicState corresponding to a block hash that has been inserted
	// but not Accepted or Rejected yet.
	GetVerifiedAtomicState(blockHash common.Hash) (AtomicState, error)

	// AtomicTrie returns the atomic trie managed by this backend.
	AtomicTrie() AtomicTrie

	// ApplyToSharedMemory applies the atomic operations that have been indexed into the trie
	// but not yet applied to shared memory for heights less than or equal to [lastAcceptedBlock].
	// This executes operations in the range [cursorHeight+1, lastAcceptedBlock].
	// The cursor is initially set by  MarkApplyToSharedMemoryCursor to signal to the atomic trie
	// the range of operations that were added to the trie without being executed on shared memory.
	ApplyToSharedMemory(lastAcceptedBlock uint64) error

	// MarkApplyToSharedMemoryCursor marks the atomic trie as containing atomic ops that
	// have not been executed on shared memory starting at [previousLastAcceptedHeight+1].
	// This is used when state sync syncs the atomic trie, such that the atomic operations
	// from [previousLastAcceptedHeight+1] to the [lastAcceptedHeight] set by state sync
	// will not have been executed on shared memory.
	MarkApplyToSharedMemoryCursor(previousLastAcceptedHeight uint64) error

	// Syncer creates and returns a new Syncer object that can be used to sync the
	// state of the atomic trie from peers
	Syncer(client syncclient.LeafClient, targetRoot common.Hash, targetHeight uint64) (Syncer, error)

	// SetLastAccepted is used after state-sync to reset the last accepted block.
	SetLastAccepted(lastAcceptedHash common.Hash)
}

// AtomicState is an abstraction created through AtomicBackend
// and can be used to apply the VM's state change for atomic txs
// or reject them to free memory.
// The root of the atomic trie after applying the state change
// is accessible through this interface as well.
type AtomicState interface {
	// Root of the atomic trie after applying the state change.
	Root() common.Hash
	// Accept applies the state change to VM's persistent storage.
	Accept() error
	// Reject frees memory associated with the state change.
	Reject() error
}

// atomicBackend implements the AtomicBackend interface using
// the AtomicTrie, AtomicTxRepository, and the VM's shared memory.
type atomicBackend struct {
	codec        codec.Manager
	bonusBlocks  map[uint64]ids.ID   // Map of height to blockID for blocks to skip indexing
	db           *versiondb.Database // Underlying database
	metadataDB   database.Database   // Underlying database containing the atomic trie metadata
	sharedMemory atomic.SharedMemory

	repo       AtomicTxRepository
	atomicTrie AtomicTrie

	lastAcceptedHash common.Hash
	verifiedRoots    map[common.Hash]AtomicState
}

// NewAtomicBackend creates an AtomicBackend from the specified dependencies
func NewAtomicBackend(
	db *versiondb.Database, sharedMemory atomic.SharedMemory,
	bonusBlocks map[uint64]ids.ID, repo AtomicTxRepository,
	lastAcceptedHeight uint64, lastAcceptedHash common.Hash, commitInterval uint64,
) (AtomicBackend, error) {
	atomicTrieDB := prefixdb.New(atomicTrieDBPrefix, db)
	metadataDB := prefixdb.New(atomicTrieMetaDBPrefix, db)
	codec := repo.Codec()

	atomicTrie, err := newAtomicTrie(atomicTrieDB, metadataDB, codec, lastAcceptedHeight, commitInterval)
	if err != nil {
		return nil, err
	}
	atomicBackend := &atomicBackend{
		codec:            codec,
		db:               db,
		metadataDB:       metadataDB,
		sharedMemory:     sharedMemory,
		bonusBlocks:      bonusBlocks,
		repo:             repo,
		atomicTrie:       atomicTrie,
		lastAcceptedHash: lastAcceptedHash,
		verifiedRoots:    make(map[common.Hash]AtomicState),
	}

	// We call ApplyToSharedMemory here to ensure that if the node was shut down in the middle
	// of applying atomic operations from state sync, we finish the operation to ensure we never
	// return an atomic trie that is out of sync with shared memory.
	// In normal operation, the cursor is not set, such that this call will be a no-op.
	if err := atomicBackend.ApplyToSharedMemory(lastAcceptedHeight); err != nil {
		return nil, err
	}
	return atomicBackend, atomicBackend.initialize(lastAcceptedHeight)
}

// initializes the atomic trie using the atomic repository height index.
// Iterating from the last committed height to the last height indexed
// in the atomic repository, making a single commit at the
// most recent height divisible by the commitInterval.
// Subsequent updates to this trie are made using the Index call as blocks are accepted.
// Note: this method assumes no atomic txs are applied at genesis.
func (a *atomicBackend) initialize(lastAcceptedHeight uint64) error {
	start := time.Now()

	// track the last committed height and last committed root
	lastCommittedRoot, lastCommittedHeight := a.atomicTrie.LastCommitted()
	log.Info("initializing atomic trie", "lastCommittedHeight", lastCommittedHeight)

	// iterate by height, from [lastCommittedHeight+1] to [lastAcceptedBlockNumber]
	height := lastCommittedHeight
	iter := a.repo.IterateByHeight(lastCommittedHeight + 1)
	defer iter.Release()

	heightsIndexed := 0
	lastUpdate := time.Now()

	// open the atomic trie at the last committed root
	tr, err := a.atomicTrie.OpenTrie(lastCommittedRoot)
	if err != nil {
		return err
	}

	for iter.Next() {
		// Get the height and transactions for this iteration (from the key and value, respectively)
		// iterate over the transactions, indexing them if the height is < commit height
		// otherwise, add the atomic operations from the transaction to the uncommittedOpsMap
		height = binary.BigEndian.Uint64(iter.Key())
		txs, err := ExtractAtomicTxs(iter.Value(), true, a.codec)
		if err != nil {
			return err
		}

		// combine atomic operations from all transactions at this block height
		combinedOps, err := mergeAtomicOps(txs)
		if err != nil {
			return err
		}

		if _, skipBonusBlock := a.bonusBlocks[height]; skipBonusBlock {
			// If [height] is a bonus block, do not index the atomic operations into the trie
			continue
		}
		if err := a.atomicTrie.UpdateTrie(tr, height, combinedOps); err != nil {
			return err
		}
		root, _, err := tr.Commit(nil)
		if err != nil {
			return err
		}
		if err := a.atomicTrie.InsertTrie(root); err != nil {
			return err
		}
		if err := a.atomicTrie.AcceptTrie(height, root); err != nil {
			return err
		}
		// If accepting this root caused a commit, also
		// flush pending changes from versiondb to disk
		// to preserve progress.
		newCommitRoot, newCommitHeight := a.atomicTrie.LastCommitted()
		if newCommitHeight > lastCommittedHeight {
			if err := a.db.Commit(); err != nil {
				return err
			}

			lastCommittedHeight = newCommitHeight
			lastCommittedRoot = newCommitRoot
		}

		heightsIndexed++

		if time.Since(lastUpdate) > progressLogUpdate {
			log.Info("imported entries into atomic trie", "heightsIndexed", heightsIndexed)
			lastUpdate = time.Now()
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}

	// check if there are accepted blocks after the last block with accepted atomic txs.
	if lastAcceptedHeight > height {
		lastAcceptedRoot := a.atomicTrie.LastAcceptedRoot()
		if err := a.atomicTrie.InsertTrie(lastAcceptedRoot); err != nil {
			return err
		}
		if err := a.atomicTrie.AcceptTrie(lastAcceptedHeight, lastAcceptedRoot); err != nil {
			return err
		}
	}

	log.Info(
		"finished initializing atomic trie",
		"lastAcceptedHeight", lastAcceptedHeight,
		"lastAcceptedAtomicRoot", a.atomicTrie.LastAcceptedRoot(),
		"heightsIndexed", heightsIndexed,
		"lastCommittedRoot", lastCommittedRoot,
		"lastCommittedHeight", lastCommittedHeight,
		"time", time.Since(start),
	)
	return nil
}

// ApplyToSharedMemory applies the atomic operations that have been indexed into the trie
// but not yet applied to shared memory for heights less than or equal to [lastAcceptedBlock].
// This executes operations in the range [cursorHeight+1, lastAcceptedBlock].
// The cursor is initially set by  MarkApplyToSharedMemoryCursor to signal to the atomic trie
// the range of operations that were added to the trie without being executed on shared memory.
func (a *atomicBackend) ApplyToSharedMemory(lastAcceptedBlock uint64) error {
	sharedMemoryCursor, err := a.metadataDB.Get(appliedSharedMemoryCursorKey)
	if err == database.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}

	lastCommittedRoot, _ := a.atomicTrie.LastCommitted()
	log.Info("applying atomic operations to shared memory", "root", lastCommittedRoot, "lastAcceptedBlock", lastAcceptedBlock, "startHeight", binary.BigEndian.Uint64(sharedMemoryCursor[:wrappers.LongLen]))

	it, err := a.atomicTrie.Iterator(lastCommittedRoot, sharedMemoryCursor)
	if err != nil {
		return err
	}
	lastUpdate := time.Now()
	putRequests, removeRequests := 0, 0
	totalPutRequests, totalRemoveRequests := 0, 0

	// value of sharedMemoryCursor is either a uint64 signifying the
	// height iteration should begin at or is a uint64+blockchainID
	// specifying the last atomic operation that was applied to shared memory.
	// To avoid applying the same operation twice, we call [it.Next()] in the
	// latter case.
	if len(sharedMemoryCursor) > wrappers.LongLen {
		it.Next()
	}

	batchOps := make(map[ids.ID]*atomic.Requests)
	for it.Next() {
		height := it.BlockNumber()
		atomicOps := it.AtomicOps()

		if height > lastAcceptedBlock {
			log.Warn("Found height above last accepted block while applying operations to shared memory", "height", height, "lastAcceptedBlock", lastAcceptedBlock)
			break
		}

		putRequests += len(atomicOps.PutRequests)
		removeRequests += len(atomicOps.RemoveRequests)
		totalPutRequests += len(atomicOps.PutRequests)
		totalRemoveRequests += len(atomicOps.RemoveRequests)
		if time.Since(lastUpdate) > 10*time.Second {
			log.Info("atomic trie iteration", "height", height, "puts", totalPutRequests, "removes", totalRemoveRequests)
			lastUpdate = time.Now()
		}
		mergeAtomicOpsToMap(batchOps, it.BlockchainID(), atomicOps)

		if putRequests+removeRequests > sharedMemoryApplyBatchSize {
			// Update the cursor to the key of the atomic operation being executed on shared memory.
			// If the node shuts down in the middle of this function call, ApplyToSharedMemory will
			// resume operation starting at the key immediately following [it.Key()].
			if err = a.metadataDB.Put(appliedSharedMemoryCursorKey, it.Key()); err != nil {
				return err
			}
			batch, err := a.db.CommitBatch()
			if err != nil {
				return err
			}
			// calling [sharedMemory.Apply] updates the last applied pointer atomically with the shared memory operation.
			if err = a.sharedMemory.Apply(batchOps, batch); err != nil {
				return err
			}
			putRequests, removeRequests = 0, 0
			batchOps = make(map[ids.ID]*atomic.Requests)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}

	if err = a.metadataDB.Delete(appliedSharedMemoryCursorKey); err != nil {
		return err
	}
	batch, err := a.db.CommitBatch()
	if err != nil {
		return err
	}
	if err = a.sharedMemory.Apply(batchOps, batch); err != nil {
		return err
	}
	log.Info("finished applying atomic operations", "puts", totalPutRequests, "removes", totalRemoveRequests)
	return nil
}

// MarkApplyToSharedMemoryCursor marks the atomic trie as containing atomic ops that
// have not been executed on shared memory starting at [previousLastAcceptedHeight+1].
// This is used when state sync syncs the atomic trie, such that the atomic operations
// from [previousLastAcceptedHeight+1] to the [lastAcceptedHeight] set by state sync
// will not have been executed on shared memory.
func (a *atomicBackend) MarkApplyToSharedMemoryCursor(previousLastAcceptedHeight uint64) error {
	// Set the cursor to [previousLastAcceptedHeight+1] so that we begin the iteration at the
	// first item that has not been applied to shared memory.
	return database.PutUInt64(a.metadataDB, appliedSharedMemoryCursorKey, previousLastAcceptedHeight+1)
}

// Syncer creates and returns a new Syncer object that can be used to sync the
// state of the atomic trie from peers
func (a *atomicBackend) Syncer(client syncclient.LeafClient, targetRoot common.Hash, targetHeight uint64) (Syncer, error) {
	return newAtomicSyncer(client, a, targetRoot, targetHeight)
}

func (a *atomicBackend) GetVerifiedAtomicState(blockHash common.Hash) (AtomicState, error) {
	if state, ok := a.verifiedRoots[blockHash]; ok {
		return state, nil
	}
	return nil, fmt.Errorf("attempt to access atomic state for an invalid block: %s", blockHash)
}

// getAtomicRootAt returns the atomic trie root for a block that is either:
// - the last accepted block
// - a block that has been verified but not accepted or rejected yet.
// If [blockHash] is neither of the above, an error is returned.
func (a *atomicBackend) getAtomicRootAt(blockHash common.Hash) (common.Hash, error) {
	// TODO: we can implement this in a few ways.
	if blockHash == a.lastAcceptedHash {
		return a.atomicTrie.LastAcceptedRoot(), nil
	}
	state, err := a.GetVerifiedAtomicState(blockHash)
	if err != nil {
		return common.Hash{}, nil
	}
	return state.Root(), nil
}

// SetLastAccepted is used after state-sync to update the last accepted block hash.
func (a *atomicBackend) SetLastAccepted(lastAcceptedHash common.Hash) {
	a.lastAcceptedHash = lastAcceptedHash
}

// CalculateRootWithTxs calculates the root of the atomic trie that would
// result from applying [txs] to the atomic trie, starting at the state
// corresponding to previously verified block [parentHash].
// The atomic trie used to calculate the root is not pinned in memory.
func (a *atomicBackend) CalculateRootWithTxs(blockHeight uint64, parentHash common.Hash, txs []*Tx) (common.Hash, error) {
	// access the atomic trie at the parent block
	parentRoot, err := a.getAtomicRootAt(parentHash)
	if err != nil {
		return common.Hash{}, err
	}
	tr, err := a.atomicTrie.OpenTrie(parentRoot)
	if err != nil {
		return common.Hash{}, err
	}

	// update the atomic trie
	atomicOps, err := mergeAtomicOps(txs)
	if err != nil {
		return common.Hash{}, err
	}
	if err := a.atomicTrie.UpdateTrie(tr, blockHeight, atomicOps); err != nil {
		return common.Hash{}, err
	}

	// return the root without pinning the trie to memory
	return tr.Hash(), nil
}

// InsertTxs returns an AtomicState that can be used to transition the VM's
// atomic state by applying [txs] to the atomic trie, starting at the state
// corresponding to previously verified block [parentHash].
// The modified atomic trie is pinned in memory and it's the caller's
// responsibility to call either Accept or Reject on the returned AtomicState
// to commit the changes or abort them and free memory.
func (a *atomicBackend) InsertTxs(blockHash common.Hash, blockHeight uint64, parentHash common.Hash, txs []*Tx) (AtomicState, error) {
	// access the atomic trie at the parent block
	parentRoot, err := a.getAtomicRootAt(parentHash)
	if err != nil {
		return nil, err
	}
	tr, err := a.atomicTrie.OpenTrie(parentRoot)
	if err != nil {
		return nil, err
	}

	// update the atomic trie
	atomicOps, err := mergeAtomicOps(txs)
	if err != nil {
		return nil, err
	}
	if err := a.atomicTrie.UpdateTrie(tr, blockHeight, atomicOps); err != nil {
		return nil, err
	}

	// get the new root and pin the atomic trie changes in memory.
	root, _, err := tr.Commit(nil)
	if err != nil {
		return nil, err
	}
	if err := a.atomicTrie.InsertTrie(root); err != nil {
		return nil, err
	}
	atomicState := &atomicState{
		backend:     a,
		blockHash:   blockHash,
		blockHeight: blockHeight,
		txs:         txs,
		atomicOps:   atomicOps,
		atomicRoot:  root,
	}

	// track this block so further blocks can be inserted on top
	// of this block
	a.verifiedRoots[blockHash] = atomicState

	// return the AtomicState interface which allows the caller
	// to Accept or Reject the atomic state changes.
	return atomicState, nil
}

func (a *atomicBackend) AtomicTrie() AtomicTrie {
	return a.atomicTrie
}

// atomicState implements the AtomicState interface using
// a pointer to the atomicBackend.
type atomicState struct {
	backend     *atomicBackend
	blockHash   common.Hash
	blockHeight uint64
	txs         []*Tx
	atomicOps   map[ids.ID]*atomic.Requests
	atomicRoot  common.Hash
}

func (a *atomicState) Root() common.Hash {
	return a.atomicRoot
}

// Accept applies the state change to VM's persistent storage.
func (a *atomicState) Accept() error {
	// Update the atomic tx repository. Note it is necessary to invoke
	// the correct method taking bonus blocks into consideration.
	if a.isBonus() {
		if err := a.backend.repo.WriteBonus(a.blockHeight, a.txs); err != nil {
			return err
		}
	} else {
		if err := a.backend.repo.Write(a.blockHeight, a.txs); err != nil {
			return err
		}
	}

	// Accept the root of this atomic trie (will be persisted if at a commit interval)
	if err := a.backend.atomicTrie.AcceptTrie(a.blockHeight, a.atomicRoot); err != nil {
		return err
	}
	// Update the last accepted block to this block and remove it from
	// the map tracking undecided blocks.
	a.backend.lastAcceptedHash = a.blockHash
	delete(a.backend.verifiedRoots, a.blockHash)

	// If this is a bonus block, commit the database without applying atomic ops
	// to shared memory.
	if a.isBonus() {
		log.Info("skipping atomic tx acceptance on bonus block", "block", a.blockHash)
		return a.backend.db.Commit()
	}

	// Otherwise, atomically commit pending changes in the version db with
	// atomic ops to shared memory.
	batch, err := a.backend.db.CommitBatch()
	if err != nil {
		return fmt.Errorf("failed to create commit batch due to: %w", err)
	}
	return a.backend.sharedMemory.Apply(a.atomicOps, batch)
}

// Reject frees memory associated with the state change.
func (a *atomicState) Reject() error {
	// Remove the block from the map of undecided blocks.
	delete(a.backend.verifiedRoots, a.blockHash)
	// Unpin the rejected atomic trie root from memory.
	return a.backend.atomicTrie.RejectTrie(a.atomicRoot)
}

// isBonus returns true if the block for atomicState is a bonus block
func (a *atomicState) isBonus() bool {
	if bonusID, found := a.backend.bonusBlocks[a.blockHeight]; found {
		return bonusID == ids.ID(a.blockHash)
	}
	return false
}
