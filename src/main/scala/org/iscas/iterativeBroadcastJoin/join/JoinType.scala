package org.iscas.iterativeBroadcastJoin.join

sealed trait JoinType

final case class IterativeBroadcastJoinType() extends JoinType

final case class SortMergeJoinType() extends JoinType