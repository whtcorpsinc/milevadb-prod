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

package plugin

import (
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/errno"
)

var (
	errInvalidPluginID         = terror.ClassPlugin.NewStd(errno.ErrInvalidPluginID)
	errInvalidPluginManifest   = terror.ClassPlugin.NewStd(errno.ErrInvalidPluginManifest)
	errInvalidPluginName       = terror.ClassPlugin.NewStd(errno.ErrInvalidPluginName)
	errInvalidPluginVersion    = terror.ClassPlugin.NewStd(errno.ErrInvalidPluginVersion)
	errDuplicatePlugin         = terror.ClassPlugin.NewStd(errno.ErrDuplicatePlugin)
	errInvalidPluginSysVarName = terror.ClassPlugin.NewStd(errno.ErrInvalidPluginSysVarName)
	errRequireVersionCheckFail = terror.ClassPlugin.NewStd(errno.ErrRequireVersionCheckFail)
)
