# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
# Copyright (c) 2016-present, Facebook, Inc. All rights reserved.
from typing import List, Optional

from fquery.fgraphql import field, graphql, obj, root

from .mock_user import MockReview, MockUser, ReviewQuery, UserQuery


@obj
class GraphQLMockUser(MockUser):
    @field
    async def friends(self) -> List["GraphQLMockUser"]:
        _users = [u async for u in MockUser.friends(self)][0]
        return [GraphQLMockUser(u.id, u.name, u.age) for u in _users]

    @field
    async def reviews(self) -> List["GraphQLMockReview"]:
        _reviews = [r async for r in MockUser.reviews(self)][0]
        return [GraphQLMockReview(r.id, r.business, r.rating) for r in _reviews]


@obj
class GraphQLMockReview(MockReview):
    @field
    async def author(self) -> "GraphQLMockUser":
        a = [r async for r in MockReview.author(self)][0]
        return GraphQLMockUser(a.id, a.name, a.age)


# Adding these decorators to UserQuery makes it unhashable
@root
@graphql
class GraphQLUserQuery:
    @field
    def user(id: int) -> GraphQLMockUser:
        return UserQuery.TYPE.get(id)

    @field
    async def users(ids: Optional[List[int]] = None) -> List[GraphQLMockUser]:
        if not ids:
            # A proxy for "all" users
            ids = range(1, 5)
        _users = await UserQuery(ids).as_list()
        return [GraphQLMockUser(u.id, u.name, u.age) for u in _users]


@graphql
class GraphQLReviewQuery:
    @field
    def review(id: int) -> GraphQLMockReview:
        return ReviewQuery.TYPE.get(id)
