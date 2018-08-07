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


package expr

type BoolExpr interface{
	isBoolExpr()
	Doopt() BoolExpr
}

func(And) isBoolExpr() {}
func(Or) isBoolExpr() {}
func(Eq) isBoolExpr() {}
func(Bool) isBoolExpr() {}

func isX(be BoolExpr,x bool) bool {
	b,ok := be.(Bool)
	return ok&&(bool(b)==x)
}


type And []BoolExpr
func (a *And) optimize(b BoolExpr) {
	if c,ok := b.(And); ok {
		for _,d := range c { a.optimize(d) }
	} else {
		*a = append(*a,b.Doopt())
	}
}
func (a And) Doopt() BoolExpr {
	out := make(And,0,len(a))
	out.optimize(a)
	nout := out[:0]
	for _,e := range out {
		//e = e.Doopt()
		if isX(e,false) { return Bool(false) }
		if isX(e,true) { continue }
		nout = append(nout,e)
	}
	switch len(nout) {
	case 0: return Bool(true)
	case 1: return nout[0]
	}
	return nout
}

type Or []BoolExpr
func (a *Or) optimize(b BoolExpr) {
	if c,ok := b.(Or); ok {
		for _,d := range c { a.optimize(d) }
	} else {
		*a = append(*a,b.Doopt())
	}
}
func (a Or) Doopt() BoolExpr {
	out := make(Or,0,len(a))
	out.optimize(a)
	nout := out[:0]
	for _,e := range out {
		//e = e.Doopt()
		if isX(e,true) { return Bool(true) }
		if isX(e,false) { continue }
		nout = append(nout,e)
	}
	switch len(nout) {
	case 0: return Bool(false)
	case 1: return nout[0]
	}
	return nout
}

type Eq struct{
	Field int
	Value interface{}
}
func (e Eq) Doopt() BoolExpr { return e }

type Bool bool
func (b Bool) Doopt() BoolExpr { return b }


