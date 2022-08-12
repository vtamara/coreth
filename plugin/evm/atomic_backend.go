// (c) 2020-2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package evm

import (
	"fmt"

	"github.com/ava-labs/avalanchego/chains/atomic"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/database/versiondb"
	"github.com/ava-labs/avalanchego/ids"
	syncclient "github.com/ava-labs/coreth/sync/client"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

var (
	_ AtomicBackend = &atomicBackend{}
	_ AtomicState   = &atomicState{}
)

// AtomicStateGetter contains methods that allow the atomic backend
// access to the root of the atomic trie at previously verified blocks.
// The VM also provides access to the last accepted block's ID
// since decided blocks may not be cached.
type AtomicStateGetter interface {
	// GetAtomicState returns the AtomicState that corresponds to the verified
	// block with the provided ID.
	// TODO: we can have this get the block instead and introduce an intermediary
	// interface such as AtomicBlock.
	GetAtomicState(ids.ID) (AtomicState, error)

	// LastAccepted returns the ID of the last accepted block.
	//
	// If no blocks have been accepted by consensus yet, it is assumed there is
	// a definitionally accepted block, the Genesis block, that will be
	// returned.
	LastAccepted() (ids.ID, error)
}

// AtomicBackend abstracts the verification and processing
// of atomic transactions
type AtomicBackend interface {
	// InsertTxs returns an AtomicState that can be used to transition the VM's
	// atomic state by applying [txs] to the atomic trie, starting at the state
	// corresponding to previously verified block [parentHash].
	// The modified atomic trie is pinned in memory and it's the caller's
	// responsibility to call either Accept or Reject on the returned AtomicState
	// to commit the changes or abort them and free memory.
	InsertTxs(blockHash common.Hash, blockHeight uint64, parentHash common.Hash, txs []*Tx) (AtomicState, error)

	// CalculateRootWithTxs calculates the root of the atomic trie that would
	// result from applying [txs] to the atomic trie, starting at the state
	// corresponding to previously verified block [parentHash].
	// The atomic trie used to calculate the root is not pinned in memory.
	CalculateRootWithTxs(blockHeight uint64, parentHash common.Hash, txs []*Tx) (common.Hash, error)

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
	bonusBlocks  map[uint64]ids.ID   // Map of height to blockID for blocks to skip indexing
	db           *versiondb.Database // Underlying database
	metadataDB   database.Database   // Underlying database containing the atomic trie metadata
	sharedMemory atomic.SharedMemory

	stateGetter AtomicStateGetter
	repo        AtomicTxRepository
	atomicTrie  AtomicTrie
}

// NewAtomicBackend creates an AtomicBackend from the specified dependencies
func NewAtomicBackend(
	db *versiondb.Database, sharedMemory atomic.SharedMemory,
	bonusBlocks map[uint64]ids.ID, repo AtomicTxRepository,
	lastAcceptedHeight uint64, commitInterval uint64,
	stateGetter AtomicStateGetter,
) (AtomicBackend, error) {
	atomicTrieDB := prefixdb.New(atomicTrieDBPrefix, db)
	metadataDB := prefixdb.New(atomicTrieMetaDBPrefix, db)

	atomicTrie, err := newAtomicTrie(atomicTrieDB, metadataDB, repo.Codec(), lastAcceptedHeight, commitInterval)
	if err != nil {
		return nil, err
	}
	atomicBackend := &atomicBackend{
		db:           db,
		metadataDB:   metadataDB,
		sharedMemory: sharedMemory,
		bonusBlocks:  bonusBlocks,
		repo:         repo,
		atomicTrie:   atomicTrie,
		stateGetter:  stateGetter,
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

// getAtomicRootAt returns the atomic trie root for a block that is either:
// - the last accepted block
// - a block that has been verified but not accepted or rejected yet.
// If [blockHash] is neither of the above, an error is returned.
func (a *atomicBackend) getAtomicRootAt(blockHash ids.ID) (common.Hash, error) {
	lastAcceptedHash, err := a.stateGetter.LastAccepted()
	if err != nil {
		return common.Hash{}, err
	}
	if blockHash == lastAcceptedHash {
		return a.atomicTrie.LastAcceptedRoot(), nil
	}
	atomicState, err := a.stateGetter.GetAtomicState(blockHash)
	if err != nil {
		return common.Hash{}, err
	}
	return atomicState.Root(), nil
}

// CalculateRootWithTxs calculates the root of the atomic trie that would
// result from applying [txs] to the atomic trie, starting at the state
// corresponding to previously verified block [parentHash].
// The atomic trie used to calculate the root is not pinned in memory.
func (a *atomicBackend) CalculateRootWithTxs(blockHeight uint64, parentHash common.Hash, txs []*Tx) (common.Hash, error) {
	// access the atomic trie at the parent block
	parentRoot, err := a.getAtomicRootAt(ids.ID(parentHash))
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
	if err := tr.UpdateTrie(blockHeight, atomicOps); err != nil {
		return common.Hash{}, err
	}

	// return the root without pinning the trie to memory
	return tr.Root(), nil
}

// InsertTxs returns an AtomicState that can be used to transition the VM's
// atomic state by applying [txs] to the atomic trie, starting at the state
// corresponding to previously verified block [parentHash].
// The modified atomic trie is pinned in memory and it's the caller's
// responsibility to call either Accept or Reject on the returned AtomicState
// to commit the changes or abort them and free memory.
func (a *atomicBackend) InsertTxs(blockHash common.Hash, blockHeight uint64, parentHash common.Hash, txs []*Tx) (AtomicState, error) {
	// access the atomic trie at the parent block
	parentRoot, err := a.getAtomicRootAt(ids.ID(parentHash))
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
	if err := tr.UpdateTrie(blockHeight, atomicOps); err != nil {
		return nil, err
	}

	// get the new root and pin the atomic trie changes in memory.
	root, err := tr.Commit()
	if err != nil {
		return nil, err
	}
	if err := a.atomicTrie.InsertTrie(root); err != nil {
		return nil, err
	}

	// return the AtomicState interface which allows the caller
	// to Accept or Reject the atomic state changes.
	return &atomicState{
		backend:     a,
		blockHash:   blockHash,
		blockHeight: blockHeight,
		txs:         txs,
		atomicOps:   atomicOps,
		atomicRoot:  root,
	}, nil
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
