// create node
// load customer.csv


// load region.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('region_id', Vertex.class)

println("load region.csv")
timeStart = System.currentTimeMillis()
new File('region.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (R_REGIONKEY, R_NAME, R_COMMENT) = line.split('\\|')
        // println(line)
        // println(R_REGIONKEY)
        // println(R_REGIONKEY.getClass())
        // println(R_REGIONKEY.isInteger())
        // println(R_NAME)
        // println(R_NAME.getClass())
        // println(R_NAME.isInteger())
        // println(R_COMMENT)
        // println(R_COMMENT.getClass())
        // println(R_COMMENT.isInteger())
        g.V().has('region', 'region_id', R_REGIONKEY).fold().coalesce(
            unfold(), 
            addV('region')
                .property('region_id', R_REGIONKEY)
                .property('R_NAME', R_NAME)
                .property('R_COMMENT', R_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println(timeEnd - timeStart)
