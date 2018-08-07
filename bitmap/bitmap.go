/*
   Copyright 2018 Simon Schmidt

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


package bitmap

import "github.com/byte-mug/columnstore/columns"
import "github.com/RoaringBitmap/roaring"
import "sync"
import "github.com/spf13/cast"
import "math"

type Index interface{
	Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap
	Set(i uint32,val interface{})
	Clear(i uint32)
}

func RunOptimize(i Index) {
	r,ok := i.(interface{RunOptimize()})
	if !ok { return }
	r.RunOptimize()
}

type NoIndex struct{}
func (n *NoIndex) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return def.Clone()
}
func (n *NoIndex) Set(i uint32,val interface{}) {}
func (n *NoIndex) Clear(i uint32) {}


type BoolIndex struct{
	sync.RWMutex
	BM *roaring.Bitmap
}
func NewBoolIndex() *BoolIndex {
	return &BoolIndex{
		BM: roaring.New(),
	}
}
var _ Index = (*BoolIndex)(nil)
func (a *BoolIndex) Set(i uint32,v interface{}) {
	a.Lock(); defer a.Unlock()
	a.iSet(i,v)
}
func (a *BoolIndex) iSet(i uint32,v interface{}) {
	if cast.ToBool(v) {
		a.BM.Remove(i)
	} else {
		a.BM.Add(i)
	}
}
func (a *BoolIndex) Clear(i uint32) {
	a.Lock(); defer a.Unlock()
	a.BM.Remove(i)
}
func (a *BoolIndex) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	if cast.ToBool(val) {
		ba := column.(*columns.BoolArray)
		ba.RLock(); defer ba.RUnlock()
		return ba.BM.Clone()
	}
	a.RLock(); defer a.RUnlock()
	return a.BM.Clone()
}
func (a *BoolIndex) RunOptimize() {
	a.Lock(); defer a.Unlock()
	a.BM.RunOptimize()
}

func strfnv(s string) (r uint64) {
	r = 14695981039346656037
	const p = 1099511628211
	for _,b := range []byte(s) {
		r ^= uint64(b)
		r *= p
	}
	return
}
func inthash64(v uint64) (r [6]uint64) {
	const n = 6
	m := uint64(1024)
	for i := range r {
		r[i] = v%m
		v/=m
		m--
	}
	for i := range r {
		cv := r[i]
		for j := i+1 ; j<n ; j++ {
			if r[j]==cv { r[j]++ }
		}
	}
	return r
}

type Internal64Index struct{
	sync.RWMutex
	BMPS[1024] *roaring.Bitmap
}
func (s *Internal64Index) Iset(i uint32,v uint64) {
	s.Lock(); defer s.Unlock()
	hsh := inthash64(v)
	for j := range s.BMPS { s.BMPS[j].Remove(i) }
	for _,j := range hsh { s.BMPS[j].Add(i) }
}
func (s *Internal64Index) Clear(i uint32) {
	s.Lock(); defer s.Unlock()
	for j := range s.BMPS { s.BMPS[j].Remove(i) }
}
func (s *Internal64Index) Ilookup(column columns.Array, val uint64, def *roaring.Bitmap) *roaring.Bitmap {
	s.RLock(); defer s.RUnlock()
	hsh := inthash64(val)
	var R [6]*roaring.Bitmap
	for i,j := range hsh { R[i] = s.BMPS[j] }
	return roaring.ParAnd(0,R[:]...)
}
func NewInternal64Index() *Internal64Index {
	si := new(Internal64Index)
	for i := range si.BMPS {
		si.BMPS[i] = roaring.New()
	}
	return si
}
func (s *Internal64Index) RunOptimize() {
	s.Lock(); defer s.Unlock()
	var wg sync.WaitGroup
	wg.Add(1024)
	f := func(i int){ defer wg.Done(); s.BMPS[i].RunOptimize() }
	for i := range s.BMPS { go f(i) }
	wg.Wait()
}

type StringIndex struct{
	*Internal64Index
}
var _ Index = StringIndex{}
func (s StringIndex) Set(i uint32,v interface{}) {
	s.Iset(i,strfnv(cast.ToString(v)))
}
func (s StringIndex) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,strfnv(cast.ToString(val)),def)
}


type Int64Index struct{
	*Internal64Index
}
var _ Index = Int64Index{}
func (s Int64Index) Set(i uint32,v interface{}) {
	s.Iset(i,uint64(cast.ToInt64(v)))
}
func (s Int64Index) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,uint64(cast.ToInt64(val)),def)
}


type Uint64Index struct{
	*Internal64Index
}
var _ Index = Uint64Index{}
func (s Uint64Index) Set(i uint32,v interface{}) {
	s.Iset(i,cast.ToUint64(v))
}
func (s Uint64Index) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,cast.ToUint64(val),def)
}


type Float64Index struct{
	*Internal64Index
}
var _ Index = Float64Index{}
func (s Float64Index) Set(i uint32,v interface{}) {
	s.Iset(i,math.Float64bits(cast.ToFloat64(v)))
}
func (s Float64Index) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,math.Float64bits(cast.ToFloat64(val)),def)
}


type Float32Index struct{
	*Internal64Index
}
var _ Index = Float32Index{}
func (s Float32Index) Set(i uint32,v interface{}) {
	s.Iset(i,uint64(math.Float32bits(cast.ToFloat32(v))))
}
func (s Float32Index) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,uint64(math.Float32bits(cast.ToFloat32(val))),def)
}


type Int32Index struct{
	*Internal64Index
}
var _ Index = Int32Index{}
func (s Int32Index) Set(i uint32,v interface{}) {
	s.Iset(i,uint64(uint32(cast.ToInt32(v))))
}
func (s Int32Index) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,uint64(uint32(cast.ToInt32(val))),def)
}

type Uint32Index struct{
	*Internal64Index
}
var _ Index = Uint32Index{}
func (s Uint32Index) Set(i uint32,v interface{}) {
	s.Iset(i,uint64(cast.ToUint32(v)))
}
func (s Uint32Index) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,uint64(cast.ToUint32(val)),def)
}


type TimeIndex struct{
	*Internal64Index
}
var _ Index = TimeIndex{}
func (s TimeIndex) Set(i uint32,v interface{}) {
	s.Iset(i,uint64(cast.ToTime(v).Unix()))
}
func (s TimeIndex) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,uint64(cast.ToTime(val).Unix()),def)
}


const SecondsPerDay = 60*60*24

type DateIndex struct{
	*Internal64Index
}
var _ Index = DateIndex{}
func (s DateIndex) Set(i uint32,v interface{}) {
	s.Iset(i,uint64(cast.ToTime(v).Unix()/SecondsPerDay))
}
func (s DateIndex) Lookup(column columns.Array, val interface{}, def *roaring.Bitmap) *roaring.Bitmap {
	return s.Ilookup(column,uint64(cast.ToTime(val).Unix()/SecondsPerDay),def)
}

