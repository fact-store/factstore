package com.cassisi.openeventstore.core.impl

import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.Fact

data class FdbFact(
    val fact: Fact,
    val positionTuple: Tuple
)
