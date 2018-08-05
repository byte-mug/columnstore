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


package columns

import "github.com/spf13/cast"
import "github.com/RoaringBitmap/roaring"
import "sync"

type BoolArray struct{
	sync.RWMutex
	BM *roaring.Bitmap
	Len uint32
}
var _ Array = (*BoolArray)(nil)
func (a BoolArray) Get(i uint32) interface{} {
	a.RLock(); defer a.RUnlock()
	return a.BM.Contains(i)
}
func (a BoolArray) Set(i uint32,v interface{}) {
	a.Lock(); defer a.Unlock()
	a.iSet(i,v)
}
func (a BoolArray) iSet(i uint32,v interface{}) {
	if cast.ToBool(v) {
		a.BM.Add(i)
	} else {
		a.BM.Remove(i)
	}
}
func (a *BoolArray) Add(v interface{}) {
	a.Lock(); defer a.Unlock()
	a.iSet(a.Len,v)
	a.Len++
}


