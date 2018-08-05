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
import "github.com/RoaringBitmap/roaring"
import "sync"

type Row []interface{}

type MetaColumn struct{
	sync.Mutex
	Length uint32
	Exist, Free *roaring.Bitmap
}

type Table struct{
	*MetaColumn
	RW sync.RWMutex
	Cols  []columns.Array
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
		t.Exist.Add(pos)
		t.Dirty[i] = true
	}
}
func (t *Table) Delete(rid uint32) {
	t.RW.RLock(); defer t.RW.RUnlock()
	t.Lock(); defer t.Unlock()
	t.Exist.Remove(rid)
	t.Free.Add(rid)
}
func (t *Table) Update(rid uint32,cols []int, vals Row) {
	t.RW.RLock(); defer t.RW.RUnlock()
	for j,i := range cols {
		t.Cols[i].Set(rid,vals[j])
		t.Dirty[i] = true
	}
}


