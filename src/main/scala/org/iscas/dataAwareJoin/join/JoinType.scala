package org.iscas.dataAwareJoin.join

sealed trait JoinType

final case class SortMergeJoinType() extends JoinType

final case class SliceJoinType() extends JoinType

final case class IterativeBroadcastJoinType() extends JoinType
