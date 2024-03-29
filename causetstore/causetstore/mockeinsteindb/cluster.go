// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mockeinsteindb

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/golang/protobuf/proto"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"go.uber.org/atomic"
)

// Cluster simulates a EinsteinDB cluster. It focuses on management and the change of
// spacetime data. A Cluster mainly includes following 3 HoTTs of spacetime data:
// 1) Region: A Region is a fragment of EinsteinDB's data whose range is [start, end).
//    The data of a Region is duplicated to multiple Peers and distributed in
//    multiple Stores.
// 2) Peer: A Peer is a replica of a Region's data. All peers of a Region form
//    a group, each group elects a Leader to provide services.
// 3) CausetStore: A CausetStore is a storage/service node. Try to think it as a EinsteinDB server
//    process. Only the causetstore with request's Region's leader Peer could respond
//    to client's request.
type Cluster struct {
	sync.RWMutex
	id      uint64
	stores  map[uint64]*CausetStore
	regions map[uint64]*Region

	mvccStore MVCCStore

	// delayEvents is used to control the execution sequence of rpc requests for test.
	delayEvents map[delayKey]time.Duration
	delayMu     sync.Mutex
}

type delayKey struct {
	startTS  uint64
	regionID uint64
}

// NewCluster creates an empty cluster. It needs to be bootstrapped before
// providing service.
func NewCluster(mvccStore MVCCStore) *Cluster {
	return &Cluster{
		stores:      make(map[uint64]*CausetStore),
		regions:     make(map[uint64]*Region),
		delayEvents: make(map[delayKey]time.Duration),
		mvccStore:   mvccStore,
	}
}

// AllocID creates an unique ID in cluster. The ID could be used as either
// StoreID, RegionID, or PeerID.
func (c *Cluster) AllocID() uint64 {
	c.Lock()
	defer c.Unlock()

	return c.allocID()
}

// AllocIDs creates multiple IDs.
func (c *Cluster) AllocIDs(n int) []uint64 {
	c.Lock()
	defer c.Unlock()

	var ids []uint64
	for len(ids) < n {
		ids = append(ids, c.allocID())
	}
	return ids
}

func (c *Cluster) allocID() uint64 {
	c.id++
	return c.id
}

// GetAllRegions gets all the regions in the cluster.
func (c *Cluster) GetAllRegions() []*Region {
	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		regions = append(regions, region)
	}
	return regions
}

// GetStore returns a CausetStore's spacetime.
func (c *Cluster) GetStore(storeID uint64) *spacetimepb.CausetStore {
	c.RLock()
	defer c.RUnlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		return proto.Clone(causetstore.spacetime).(*spacetimepb.CausetStore)
	}
	return nil
}

// GetAllStores returns all Stores' spacetime.
func (c *Cluster) GetAllStores() []*spacetimepb.CausetStore {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*spacetimepb.CausetStore, 0, len(c.stores))
	for _, causetstore := range c.stores {
		stores = append(stores, proto.Clone(causetstore.spacetime).(*spacetimepb.CausetStore))
	}
	return stores
}

// StopStore stops a causetstore with storeID.
func (c *Cluster) StopStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.spacetime.State = spacetimepb.StoreState_Offline
	}
}

// StartStore starts a causetstore with storeID.
func (c *Cluster) StartStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.spacetime.State = spacetimepb.StoreState_Up
	}
}

// CancelStore makes the causetstore with cancel state true.
func (c *Cluster) CancelStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	//A causetstore returns context.Cancelled Error when cancel is true.
	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.cancel = true
	}
}

// UnCancelStore makes the causetstore with cancel state false.
func (c *Cluster) UnCancelStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.cancel = false
	}
}

// GetStoreByAddr returns a CausetStore's spacetime by an addr.
func (c *Cluster) GetStoreByAddr(addr string) *spacetimepb.CausetStore {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.stores {
		if s.spacetime.GetAddress() == addr {
			return proto.Clone(s.spacetime).(*spacetimepb.CausetStore)
		}
	}
	return nil
}

// GetAndCheckStoreByAddr checks and returns a CausetStore's spacetime by an addr
func (c *Cluster) GetAndCheckStoreByAddr(addr string) (*spacetimepb.CausetStore, error) {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.stores {
		if s.cancel {
			return nil, context.Canceled
		}
		if s.spacetime.GetAddress() == addr {
			return proto.Clone(s.spacetime).(*spacetimepb.CausetStore), nil
		}
	}
	return nil, nil
}

// AddStore add a new CausetStore to the cluster.
func (c *Cluster) AddStore(storeID uint64, addr string) {
	c.Lock()
	defer c.Unlock()

	c.stores[storeID] = newStore(storeID, addr)
}

// RemoveStore removes a CausetStore from the cluster.
func (c *Cluster) RemoveStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.stores, storeID)
}

// UFIDelateStoreAddr uFIDelates causetstore address for cluster.
func (c *Cluster) UFIDelateStoreAddr(storeID uint64, addr string, labels ...*spacetimepb.StoreLabel) {
	c.Lock()
	defer c.Unlock()
	c.stores[storeID] = newStore(storeID, addr, labels...)
}

// GetRegion returns a Region's spacetime and leader ID.
func (c *Cluster) GetRegion(regionID uint64) (*spacetimepb.Region, uint64) {
	c.RLock()
	defer c.RUnlock()

	r := c.regions[regionID]
	if r == nil {
		return nil, 0
	}
	return proto.Clone(r.Meta).(*spacetimepb.Region), r.leader
}

// GetRegionByKey returns the Region and its leader whose range contains the key.
func (c *Cluster) GetRegionByKey(key []byte) (*spacetimepb.Region, *spacetimepb.Peer) {
	c.RLock()
	defer c.RUnlock()

	for _, r := range c.regions {
		if regionContains(r.Meta.StartKey, r.Meta.EndKey, key) {
			return proto.Clone(r.Meta).(*spacetimepb.Region), proto.Clone(r.leaderPeer()).(*spacetimepb.Peer)
		}
	}
	return nil, nil
}

// GetPrevRegionByKey returns the previous Region and its leader whose range contains the key.
func (c *Cluster) GetPrevRegionByKey(key []byte) (*spacetimepb.Region, *spacetimepb.Peer) {
	c.RLock()
	defer c.RUnlock()

	currentRegion, _ := c.GetRegionByKey(key)
	if len(currentRegion.StartKey) == 0 {
		return nil, nil
	}
	for _, r := range c.regions {
		if bytes.Equal(r.Meta.EndKey, currentRegion.StartKey) {
			return proto.Clone(r.Meta).(*spacetimepb.Region), proto.Clone(r.leaderPeer()).(*spacetimepb.Peer)
		}
	}
	return nil, nil
}

// GetRegionByID returns the Region and its leader whose ID is regionID.
func (c *Cluster) GetRegionByID(regionID uint64) (*spacetimepb.Region, *spacetimepb.Peer) {
	c.RLock()
	defer c.RUnlock()

	for _, r := range c.regions {
		if r.Meta.GetId() == regionID {
			return proto.Clone(r.Meta).(*spacetimepb.Region), proto.Clone(r.leaderPeer()).(*spacetimepb.Peer)
		}
	}
	return nil, nil
}

// ScanRegions returns at most `limit` regions from given `key` and their leaders.
func (c *Cluster) ScanRegions(startKey, endKey []byte, limit int) []*fidel.Region {
	c.RLock()
	defer c.RUnlock()

	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		regions = append(regions, region)
	}

	sort.Slice(regions, func(i, j int) bool {
		return bytes.Compare(regions[i].Meta.GetStartKey(), regions[j].Meta.GetStartKey()) < 0
	})

	startPos := sort.Search(len(regions), func(i int) bool {
		if len(regions[i].Meta.GetEndKey()) == 0 {
			return true
		}
		return bytes.Compare(regions[i].Meta.GetEndKey(), startKey) > 0
	})
	regions = regions[startPos:]
	if len(endKey) > 0 {
		endPos := sort.Search(len(regions), func(i int) bool {
			return bytes.Compare(regions[i].Meta.GetStartKey(), endKey) >= 0
		})
		if endPos > 0 {
			regions = regions[:endPos]
		}
	}
	if limit > 0 && len(regions) > limit {
		regions = regions[:limit]
	}

	result := make([]*fidel.Region, 0, len(regions))
	for _, region := range regions {
		leader := region.leaderPeer()
		if leader == nil {
			leader = &spacetimepb.Peer{}
		} else {
			leader = proto.Clone(leader).(*spacetimepb.Peer)
		}

		r := &fidel.Region{
			Meta:   proto.Clone(region.Meta).(*spacetimepb.Region),
			Leader: leader,
		}
		result = append(result, r)
	}

	return result
}

// Bootstrap creates the first Region. The Stores should be in the Cluster before
// bootstrap.
func (c *Cluster) Bootstrap(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) {
	c.Lock()
	defer c.Unlock()

	if len(storeIDs) != len(peerIDs) {
		panic("len(storeIDs) != len(peerIDs)")
	}
	c.regions[regionID] = newRegion(regionID, storeIDs, peerIDs, leaderPeerID)
}

// AddPeer adds a new Peer for the Region on the CausetStore.
func (c *Cluster) AddPeer(regionID, storeID, peerID uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].addPeer(peerID, storeID)
}

// RemovePeer removes the Peer from the Region. Note that if the Peer is leader,
// the Region will have no leader before calling ChangeLeader().
func (c *Cluster) RemovePeer(regionID, storeID uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].removePeer(storeID)
}

// ChangeLeader sets the Region's leader Peer. Caller should guarantee the Peer
// exists.
func (c *Cluster) ChangeLeader(regionID, leaderPeerID uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].changeLeader(leaderPeerID)
}

// GiveUpLeader sets the Region's leader to 0. The Region will have no leader
// before calling ChangeLeader().
func (c *Cluster) GiveUpLeader(regionID uint64) {
	c.ChangeLeader(regionID, 0)
}

// Split splits a Region at the key (encoded) and creates new Region.
func (c *Cluster) Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) {
	c.SplitRaw(regionID, newRegionID, NewMvccKey(key), peerIDs, leaderPeerID)
}

// SplitRaw splits a Region at the key (not encoded) and creates new Region.
func (c *Cluster) SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) *spacetimepb.Region {
	c.Lock()
	defer c.Unlock()

	newRegion := c.regions[regionID].split(newRegionID, rawKey, peerIDs, leaderPeerID)
	c.regions[newRegionID] = newRegion
	// The mockeinsteindb should return a deep copy of spacetime info to avoid data race
	spacetime := proto.Clone(newRegion.Meta)
	return spacetime.(*spacetimepb.Region)
}

// Merge merges 2 regions, their key ranges should be adjacent.
func (c *Cluster) Merge(regionID1, regionID2 uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID1].merge(c.regions[regionID2].Meta.GetEndKey())
	delete(c.regions, regionID2)
}

// SplitTable evenly splits the data in causet into count regions.
// Only works for single causetstore.
func (c *Cluster) SplitTable(blockID int64, count int) {
	blockStart := blockcodec.GenTableRecordPrefix(blockID)
	blockEnd := blockStart.PrefixNext()
	c.splitRange(c.mvccStore, NewMvccKey(blockStart), NewMvccKey(blockEnd), count)
}

// SplitIndex evenly splits the data in index into count regions.
// Only works for single causetstore.
func (c *Cluster) SplitIndex(blockID, indexID int64, count int) {
	indexStart := blockcodec.EncodeTableIndexPrefix(blockID, indexID)
	indexEnd := indexStart.PrefixNext()
	c.splitRange(c.mvccStore, NewMvccKey(indexStart), NewMvccKey(indexEnd), count)
}

// SplitKeys evenly splits the start, end key into "count" regions.
// Only works for single causetstore.
func (c *Cluster) SplitKeys(start, end ekv.Key, count int) {
	c.splitRange(c.mvccStore, NewMvccKey(start), NewMvccKey(end), count)
}

// ScheduleDelay schedules a delay event for a transaction on a region.
func (c *Cluster) ScheduleDelay(startTS, regionID uint64, dur time.Duration) {
	c.delayMu.Lock()
	c.delayEvents[delayKey{startTS: startTS, regionID: regionID}] = dur
	c.delayMu.Unlock()
}

func (c *Cluster) handleDelay(startTS, regionID uint64) {
	key := delayKey{startTS: startTS, regionID: regionID}
	c.delayMu.Lock()
	dur, ok := c.delayEvents[key]
	if ok {
		delete(c.delayEvents, key)
	}
	c.delayMu.Unlock()
	if ok {
		time.Sleep(dur)
	}
}

func (c *Cluster) splitRange(mvccStore MVCCStore, start, end MvccKey, count int) {
	c.Lock()
	defer c.Unlock()
	c.evacuateOldRegionRanges(start, end)
	regionPairs := c.getEntriesGroupByRegions(mvccStore, start, end, count)
	c.createNewRegions(regionPairs, start, end)
}

// getEntriesGroupByRegions groups the key value pairs into splitted regions.
func (c *Cluster) getEntriesGroupByRegions(mvccStore MVCCStore, start, end MvccKey, count int) [][]Pair {
	startTS := uint64(math.MaxUint64)
	limit := math.MaxInt32
	pairs := mvccStore.Scan(start.Raw(), end.Raw(), limit, startTS, ekvrpcpb.IsolationLevel_SI, nil)
	regionEntriesSlice := make([][]Pair, 0, count)
	quotient := len(pairs) / count
	remainder := len(pairs) % count
	i := 0
	for i < len(pairs) {
		regionEntryCount := quotient
		if remainder > 0 {
			remainder--
			regionEntryCount++
		}
		regionEntries := pairs[i : i+regionEntryCount]
		regionEntriesSlice = append(regionEntriesSlice, regionEntries)
		i += regionEntryCount
	}
	return regionEntriesSlice
}

func (c *Cluster) createNewRegions(regionPairs [][]Pair, start, end MvccKey) {
	for i := range regionPairs {
		peerID := c.allocID()
		newRegion := newRegion(c.allocID(), []uint64{c.firstStoreID()}, []uint64{peerID}, peerID)
		var regionStartKey, regionEndKey MvccKey
		if i == 0 {
			regionStartKey = start
		} else {
			regionStartKey = NewMvccKey(regionPairs[i][0].Key)
		}
		if i == len(regionPairs)-1 {
			regionEndKey = end
		} else {
			// Use the next region's first key as region end key.
			regionEndKey = NewMvccKey(regionPairs[i+1][0].Key)
		}
		newRegion.uFIDelateKeyRange(regionStartKey, regionEndKey)
		c.regions[newRegion.Meta.Id] = newRegion
	}
}

// evacuateOldRegionRanges evacuate the range [start, end].
// Old regions has intersection with [start, end) will be uFIDelated or deleted.
func (c *Cluster) evacuateOldRegionRanges(start, end MvccKey) {
	oldRegions := c.getRegionsCoverRange(start, end)
	for _, oldRegion := range oldRegions {
		startCmp := bytes.Compare(oldRegion.Meta.StartKey, start)
		endCmp := bytes.Compare(oldRegion.Meta.EndKey, end)
		if len(oldRegion.Meta.EndKey) == 0 {
			endCmp = 1
		}
		if startCmp >= 0 && endCmp <= 0 {
			// The region is within causet data, it will be replaced by new regions.
			delete(c.regions, oldRegion.Meta.Id)
		} else if startCmp < 0 && endCmp > 0 {
			// A single Region covers causet data, split into two regions that do not overlap causet data.
			oldEnd := oldRegion.Meta.EndKey
			oldRegion.uFIDelateKeyRange(oldRegion.Meta.StartKey, start)
			peerID := c.allocID()
			newRegion := newRegion(c.allocID(), []uint64{c.firstStoreID()}, []uint64{peerID}, peerID)
			newRegion.uFIDelateKeyRange(end, oldEnd)
			c.regions[newRegion.Meta.Id] = newRegion
		} else if startCmp < 0 {
			oldRegion.uFIDelateKeyRange(oldRegion.Meta.StartKey, start)
		} else {
			oldRegion.uFIDelateKeyRange(end, oldRegion.Meta.EndKey)
		}
	}
}

func (c *Cluster) firstStoreID() uint64 {
	for id := range c.stores {
		return id
	}
	return 0
}

// getRegionsCoverRange gets regions in the cluster that has intersection with [start, end).
func (c *Cluster) getRegionsCoverRange(start, end MvccKey) []*Region {
	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		onRight := bytes.Compare(end, region.Meta.StartKey) <= 0
		onLeft := bytes.Compare(region.Meta.EndKey, start) <= 0
		if len(region.Meta.EndKey) == 0 {
			onLeft = false
		}
		if onLeft || onRight {
			continue
		}
		regions = append(regions, region)
	}
	return regions
}

// Region is the Region spacetime data.
type Region struct {
	Meta   *spacetimepb.Region
	leader uint64
}

func newPeerMeta(peerID, storeID uint64) *spacetimepb.Peer {
	return &spacetimepb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
}

func newRegion(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) *Region {
	if len(storeIDs) != len(peerIDs) {
		panic("len(storeIDs) != len(peerIds)")
	}
	peers := make([]*spacetimepb.Peer, 0, len(storeIDs))
	for i := range storeIDs {
		peers = append(peers, newPeerMeta(peerIDs[i], storeIDs[i]))
	}
	spacetime := &spacetimepb.Region{
		Id:    regionID,
		Peers: peers,
	}
	return &Region{
		Meta:   spacetime,
		leader: leaderPeerID,
	}
}

func (r *Region) addPeer(peerID, storeID uint64) {
	r.Meta.Peers = append(r.Meta.Peers, newPeerMeta(peerID, storeID))
	r.incConfVer()
}

func (r *Region) removePeer(peerID uint64) {
	for i, peer := range r.Meta.Peers {
		if peer.GetId() == peerID {
			r.Meta.Peers = append(r.Meta.Peers[:i], r.Meta.Peers[i+1:]...)
			break
		}
	}
	if r.leader == peerID {
		r.leader = 0
	}
	r.incConfVer()
}

func (r *Region) changeLeader(leaderID uint64) {
	r.leader = leaderID
}

func (r *Region) leaderPeer() *spacetimepb.Peer {
	for _, p := range r.Meta.Peers {
		if p.GetId() == r.leader {
			return p
		}
	}
	return nil
}

func (r *Region) split(newRegionID uint64, key MvccKey, peerIDs []uint64, leaderPeerID uint64) *Region {
	if len(r.Meta.Peers) != len(peerIDs) {
		panic("len(r.spacetime.Peers) != len(peerIDs)")
	}
	storeIDs := make([]uint64, 0, len(r.Meta.Peers))
	for _, peer := range r.Meta.Peers {
		storeIDs = append(storeIDs, peer.GetStoreId())
	}
	region := newRegion(newRegionID, storeIDs, peerIDs, leaderPeerID)
	region.uFIDelateKeyRange(key, r.Meta.EndKey)
	r.uFIDelateKeyRange(r.Meta.StartKey, key)
	return region
}

func (r *Region) merge(endKey MvccKey) {
	r.Meta.EndKey = endKey
	r.incVersion()
}

func (r *Region) uFIDelateKeyRange(start, end MvccKey) {
	r.Meta.StartKey = start
	r.Meta.EndKey = end
	r.incVersion()
}

func (r *Region) incConfVer() {
	r.Meta.RegionEpoch = &spacetimepb.RegionEpoch{
		ConfVer: r.Meta.GetRegionEpoch().GetConfVer() + 1,
		Version: r.Meta.GetRegionEpoch().GetVersion(),
	}
}

func (r *Region) incVersion() {
	r.Meta.RegionEpoch = &spacetimepb.RegionEpoch{
		ConfVer: r.Meta.GetRegionEpoch().GetConfVer(),
		Version: r.Meta.GetRegionEpoch().GetVersion() + 1,
	}
}

// CausetStore is the CausetStore's spacetime data.
type CausetStore struct {
	spacetime  *spacetimepb.CausetStore
	cancel     bool // return context.Cancelled error when cancel is true.
	tokenCount atomic.Int64
}

func newStore(storeID uint64, addr string, labels ...*spacetimepb.StoreLabel) *CausetStore {
	return &CausetStore{
		spacetime: &spacetimepb.CausetStore{
			Id:      storeID,
			Address: addr,
			Labels:  labels,
		},
	}
}
