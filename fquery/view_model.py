# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
# Copyright (c) 2016-present, Facebook, Inc. All rights reserved.
from collections import OrderedDict


class ViewModel(OrderedDict):
    """Like an OrderedDict, but treats :id as special for
       equality purposes and hashable (so you can create sets).
    """

    IDKEY = ":id"
    TYPE_KEY = ":type"

    def __lt__(self, other):
        return self[ViewModel.IDKEY] < other[ViewModel.IDKEY]

    def __eq__(self, other):
        if ViewModel.IDKEY in self:
            return self[ViewModel.IDKEY] == other[ViewModel.IDKEY]
        else:
            return dict.__eq__(self, other)

    def __repr__(self):
        return dict.__repr__(self)

    def __hash__(self) -> int:
        if ViewModel.IDKEY in self:
            return hash(self[ViewModel.IDKEY])
        else:
            return 0

    def __setitem__(self, name, value):
        if name == "id":
            name = ViewModel.IDKEY
        elif name == "_type":
            name = ViewModel.TYPE_KEY
        super().__setitem__(name, value)
