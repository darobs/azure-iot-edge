#!/bin/bash
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

build_root=$(cd "$(dirname "$0")/../v1" && pwd)
cd $build_root

# -- C --
./tools/inteledison_c.sh "$@" #-x 
[ $? -eq 0 ] || exit $?
 