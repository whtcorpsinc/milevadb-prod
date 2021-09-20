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

package dbs

import (
	"fmt"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
)


type AlterAlgorithm struct {
	// supported MUST causetstore algorithms in the order 'INSTANT, INPLACE, COPY'
	supported []ast.AlgorithmType
	// If the alter algorithm is not given, the defAlgorithm will be used.
	defAlgorithm ast.AlgorithmType
}

var (
	instantAlgorithm = &AlterAlgorithm{
		supported:    []ast.AlgorithmType{ast.AlgorithmTypeInstant},
		defAlgorithm: ast.AlgorithmTypeInstant,
	}

	inplaceAlgorithm = &AlterAlgorithm{
		supported:    []ast.AlgorithmType{ast.AlgorithmTypeInplace},
		defAlgorithm: ast.AlgorithmTypeInplace,
	}
)

func getProperAlgorithm(specify ast.AlgorithmType, algorithm *AlterAlgorithm) (ast.AlgorithmType, error) {
	if specify == ast.AlgorithmTypeDefault {
		return algorithm.defAlgorithm, nil
	}

	r := ast.AlgorithmTypeDefault

	for _, a := range algorithm.supported {
		if specify <= a {
			r = a
			break
		}
	}

	var err error
	if specify != r {
		err = ErrAlterOperationNotSupported.GenWithStackByArgs(fmt.Sprintf("ALGORITHM=%s", specify), fmt.Sprintf("Cannot alter causet by %s", specify), fmt.Sprintf("ALGORITHM=%s", algorithm.defAlgorithm))
	}
	return r, err
}

// ResolveAlterAlgorithm resolves the algorithm of the alterSpec.
// If specify is the ast.AlterAlgorithmDefault, then the default algorithm of the alter action will be returned.
// If specify algorithm is not supported by the alter action, it will try to find a better algorithm in the order `INSTANT > INPLACE > COPY`, errAlterOperationNotSupported will be returned.
// E.g. INSTANT may be returned if specify=INPLACE
// If failed to choose any valid algorithm, AlgorithmTypeDefault and errAlterOperationNotSupported will be returned
func ResolveAlterAlgorithm(alterSpec *ast.AlterBlockSpec, specify ast.AlgorithmType) (ast.AlgorithmType, error) {
	switch alterSpec.Tp {
	// For now, MilevaDB only support inplace algorithm and instant algorithm.
	case ast.AlterBlockAddConstraint:
		return getProperAlgorithm(specify, inplaceAlgorithm)
	default:
		return getProperAlgorithm(specify, instantAlgorithm)
	}
}
