# Go support for LLSN - Allyst's data interchange format.
# LLSN specification http://allyst.org/opensource/llsn/

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.

# Full license: https://github.com/allyst/go-llsn/blob/master/LICENSE

# copyright (C) 2014 Allyst Inc. http://allyst.com
# author Taras Halturin <halturin@allyst.com>

install:
	go install

test: install generate-test-pbs
	go test


generate-test-pbs:
	make install && cd testdata && make
