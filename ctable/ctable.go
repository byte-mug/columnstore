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


package ctable

import "github.com/byte-mug/columnstore/columns"
import "github.com/byte-mug/columnstore/bitmap"
import "github.com/byte-mug/columnstore/expr"
import "github.com/RoaringBitmap/roaring"

import "sync"

type Row []interface{}

type MetaColumn struct{
	sync.RWMutex
	Length uint32
	Exist, Free *roaring.Bitmap
}

type Table struct{
	*MetaColumn
	RW sync.RWMutex
	Cols  []columns.Array
	Maps  []bitmap.Index
	Dirty []bool
}
func (t *Table) Insert(row Row) {
	t.RW.RLock(); defer t.RW.RUnlock()
	t.Lock(); defer t.Unlock()
	var pos uint32
	appnd := true
	if !t.Free.IsEmpty() {
		pos = t.Free.Minimum()
		appnd = false
	} else {
		pos = t.Length
		t.Length++
	}
	for i,r := range row {
		if appnd {
			t.Cols[i].Add(r)
		} else {
			t.Cols[i].Set(pos,r)
			t.Free.Remove(pos)
		}
		t.Maps[i].Set(pos,r)
		t.Exist.Add(pos)
		t.Dirty[i] = true
	}
}
func (t *Table) Delete(rid uint32) {
	t.RW.RLock(); defer t.RW.RUnlock()
	t.Lock(); defer t.Unlock()
	t.Exist.Remove(rid)
	t.Free.Add(rid)
	for _,m := range t.Maps{ m.Clear(rid) }
}
func (t *Table) Update(rid uint32,cols []int, vals Row) {
	t.RW.RLock(); defer t.RW.RUnlock()
	for j,i := range cols {
		t.Cols[i].Set(rid,vals[j])
		t.Maps[i].Set(rid,vals[j])
		t.Dirty[i] = true
	}
}
func (t *Table) GetRow(rid uint32,cols []int, vals Row) {
	t.RW.RLock(); defer t.RW.RUnlock()
	for j,i := range cols {
		vals[j] = t.Cols[i].Get(rid)
	}
}
func (t *Table) GetEntireRow(rid uint32, vals Row) {
	t.RW.RLock(); defer t.RW.RUnlock()
	for i := range vals {
		vals[i] = t.Cols[i].Get(rid)
	}
}
func (t *Table) doOptimize1(wg *sync.WaitGroup) {
	t.Lock(); defer t.Unlock()
	defer wg.Done()
	t.Exist.RunOptimize()
	t.Free.RunOptimize()
}
func (t *Table) RunOptimize() {
	t.RW.RLock(); defer t.RW.RUnlock()
	var wg sync.WaitGroup
	wg.Add(1+len(t.Maps)+len(t.Cols))
	f := func(i int) {
		defer wg.Done()
		bitmap.RunOptimize(t.Maps[i])
	}
	g := func(i int) {
		defer wg.Done()
		columns.RunOptimize(t.Cols[i])
	}
	t.doOptimize1(&wg)
	for i := range t.Maps { go f(i); go g(i) }
	wg.Wait()
}
func (t *Table) unsyncPerform(co expr.BoolExpr) *roaring.Bitmap {
	switch v := co.(type) {
	case expr.Or:
		r := make([]*roaring.Bitmap,len(v))
		for i,e := range v {
			r[i] = t.unsyncPerform(e)
		}
		return roaring.ParOr(0,r...)
	case expr.And:
		r := make([]*roaring.Bitmap,len(v))
		for i,e := range v {
			r[i] = t.unsyncPerform(e)
		}
		return roaring.ParAnd(0,r...)
	case expr.Eq:
		return t.Maps[v.Field].Lookup(t.Cols[v.Field],v.Value,t.Exist)
	case expr.Bool:
		if v { return t.Exist.Clone() }
		return roaring.New()
	}
	return roaring.New()
}
func (t *Table) Perform(co expr.BoolExpr) *roaring.Bitmap {
	t.RW.RLock(); defer t.RW.RUnlock()
	t.RLock(); defer t.RUnlock()
	return t.unsyncPerform(co)
}


