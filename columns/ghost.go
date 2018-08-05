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

type Array interface{
	Get(i uint32) interface{}
	Set(i uint32,v interface{})
	Add(v interface{})
}

type Int64Array []int64
var _ Array = (*Int64Array)(nil)
func (a Int64Array) Get(i uint32) interface{} {
	return a[i]
}
func (a Int64Array) Set(i uint32,v interface{}) {
	a[i] = cast.ToInt64(v)
}
func (a *Int64Array) Add(v interface{}) {
	*a = append(*a,cast.ToInt64(v))
}

type Int32Array []int32
var _ Array = (*Int32Array)(nil)
func (a Int32Array) Get(i uint32) interface{} {
	return a[i]
}
func (a Int32Array) Set(i uint32,v interface{}) {
	a[i] = cast.ToInt32(v)
}
func (a *Int32Array) Add(v interface{}) {
	*a = append(*a,cast.ToInt32(v))
}

type Uint64Array []uint64
var _ Array = (*Uint64Array)(nil)
func (a Uint64Array) Get(i uint32) interface{} {
	return a[i]
}
func (a Uint64Array) Set(i uint32,v interface{}) {
	a[i] = cast.ToUint64(v)
}
func (a *Uint64Array) Add(v interface{}) {
	*a = append(*a,cast.ToUint64(v))
}

type Uint32Array []uint32
var _ Array = (*Uint32Array)(nil)
func (a Uint32Array) Get(i uint32) interface{} {
	return a[i]
}
func (a Uint32Array) Set(i uint32,v interface{}) {
	a[i] = cast.ToUint32(v)
}
func (a *Uint32Array) Add(v interface{}) {
	*a = append(*a,cast.ToUint32(v))
}

type Float64Array []float64
var _ Array = (*Float64Array)(nil)
func (a Float64Array) Get(i uint32) interface{} {
	return a[i]
}
func (a Float64Array) Set(i uint32,v interface{}) {
	a[i] = cast.ToFloat64(v)
}
func (a *Float64Array) Add(v interface{}) {
	*a = append(*a,cast.ToFloat64(v))
}

type Float32Array []float32
var _ Array = (*Float32Array)(nil)
func (a Float32Array) Get(i uint32) interface{} {
	return a[i]
}
func (a Float32Array) Set(i uint32,v interface{}) {
	a[i] = cast.ToFloat32(v)
}
func (a *Float32Array) Add(v interface{}) {
	*a = append(*a,cast.ToFloat32(v))
}

type StringArray []string
var _ Array = (*StringArray)(nil)
func (a StringArray) Get(i uint32) interface{} {
	return a[i]
}
func (a StringArray) Set(i uint32,v interface{}) {
	a[i] = cast.ToString(v)
}
func (a *StringArray) Add(v interface{}) {
	*a = append(*a,cast.ToString(v))
}

type BlobArray []string
var _ Array = (*BlobArray)(nil)
func (a BlobArray) Get(i uint32) interface{} {
	return []byte(a[i])
}
func (a BlobArray) Set(i uint32,v interface{}) {
	a[i] = cast.ToString(v)
}
func (a *BlobArray) Add(v interface{}) {
	*a = append(*a,cast.ToString(v))
}

type GenericArray []interface{}
var _ Array = (*GenericArray)(nil)
func (a GenericArray) Get(i uint32) interface{} {
	return a[i]
}
func (a GenericArray) Set(i uint32,v interface{}) {
	a[i] = v
}
func (a *GenericArray) Add(v interface{}) {
	*a = append(*a,v)
}

