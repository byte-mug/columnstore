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
import "time"

// A column that stores date-time values.
type TimeArray []int64
var _ Array = (*TimeArray)(nil)
func (a TimeArray) Get(i uint32) interface{} {
	return time.Unix(a[i],0)
}
func (a TimeArray) Set(i uint32,v interface{}) {
	a[i] = cast.ToTime(v).Unix()
}
func (a *TimeArray) Add(v interface{}) {
	*a = append(*a,cast.ToTime(v).Unix())
}

const SecondsPerDay = 60*60*24

// A column that stores dates.
type DateArray []int64
var _ Array = (*DateArray)(nil)
func (a DateArray) Get(i uint32) interface{} {
	return time.Unix(a[i]*SecondsPerDay,0)
}
func (a DateArray) Set(i uint32,v interface{}) {
	a[i] = cast.ToTime(v).Unix()/SecondsPerDay
}
func (a *DateArray) Add(v interface{}) {
	*a = append(*a,cast.ToTime(v).Unix()/SecondsPerDay)
}



