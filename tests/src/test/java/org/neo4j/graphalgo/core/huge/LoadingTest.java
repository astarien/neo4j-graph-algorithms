package org.neo4j.graphalgo.core.huge;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.MemoryUsage;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

public final class LoadingTest {

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void name() {
        db.execute("CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +

                "CREATE" +
                " (a)-[:TYPE]->(b),\n" +
                " (e)-[:TYPE]->(d),\n" +
                " (d)-[:TYPE]->(c),\n" +
                " (a)-[:TYPE]->(c),\n" +
                " (a)-[:TYPE]->(d),\n" +
                " (b)-[:TYPE]->(e),\n" +
                " (a)-[:TYPE]->(e)");

        final HugeGraph graph = (HugeGraph) new GraphLoader(db).withDirection(Direction.OUTGOING).withExecutorService(Pools.DEFAULT).load(HugeGraphFactory.class);
        System.out.println("graph = " + graph);
    }
}
