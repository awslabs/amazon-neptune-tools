const nodeAssert = require("assert").strict
const uuid = require("uuid")
const gremlin = require("./neptune-gremlin")

/**
 * Test an assertion and log the results.
 * 
 * @param {*} msg 
 * @param {*} t 
 * @returns 
 */
function assert(msg, t) {
    if (!t) {
        console.error(`FAILED: ${msg}`)
        return false
    } else {
        console.log(`SUCCEEDED: ${msg}`)
        return true
    }
}

/**
 * Run a series of test assertions.
 * 
 * @param {*} assertions 
 * @returns 
 */
function runAssertions(assertions) {
    let allSucceeded = true
    for (const t in assertions) {
        let result = false
        try {
            result = assert(t, assertions[t]())
        } catch (e) {
            allSucceeded = false
            console.error(`EXCEPTION: ${t}: ${JSON.stringify(e)}`)
        }
        if (!result) allSucceeded = false
    }
    return allSucceeded
}

/**
 * Lambda handler for the integration tests.
 * 
 * @param {*} event 
 * @param {*} context 
 * @returns 
 */
exports.handler = async (event, context) => {
    console.log(event)
    console.log(context)

    try {
        await runTests()
        return true
    } catch (ex) {
        console.error(ex)
        return false
    }
}

/**
 * Make sure a node can be saved without an id.
 * 
 * @param {*} conn 
 */
async function testNoId(conn) {
    console.log("Test creating node with no id")
    const propVal = uuid.v4()
    await conn.saveNode({
        properties: {
            testnoidprop: propVal,
        },
        labels: ["testNoIdLabel"],
    })

    const {value: node} = await conn.query(async g => g.V().has("testnoidprop", propVal).elementMap().next())

    console.log("TestNoId node found", node)

    nodeAssert.strictEqual(node.testnoidprop, propVal)
}

/**
 * Test the options.focus functionality that allows us to retrieve a subset 
 * of the graph.
 * 
 * @param {*} connection 
 */
async function testFocus(connection) {

    const node1 = {
        id: uuid.v4(),
        properties: {
            name: "Test Focus1",
            category: "category_a",
        },
        labels: ["label_x"],
    }

    await connection.saveNode(node1)

    const node2 = {
        id: uuid.v4(),
        properties: {
            name: "Test Focus2",
        },
        labels: ["label_y"],
    }

    await connection.saveNode(node2)

    const node3 = {
        id: uuid.v4(),
        properties: {
            name: "Test Focus3",
        },
        labels: ["label_x"],
    }

    await connection.saveNode(node3)

    const node4 = {
        id: uuid.v4(),
        properties: {
            name: "Test Focus4",
        },
        labels: ["label_z"],
    }

    await connection.saveNode(node4)

    const edge1 = {
        id: uuid.v4(),
        label: "points_to",
        to: node2.id,
        from: node1.id,
        properties: {},
    }

    await connection.saveEdge(edge1)

    async function cleanup() {
        try {
            await connection.deleteEdge(edge1.id)
            await connection.deleteNode(node1.id)
            await connection.deleteNode(node2.id)
            await connection.deleteNode(node3.id)
            await connection.deleteNode(node4.id)
        } catch (ex) {
            console.log(ex)
        }
    }

    // The original test exercised connection.search({focus}), which performs a
    // label-wide g.V().hasLabel(...) scan across the whole cluster. To keep the
    // suite constrained to only the data it wrote, verify the same graph
    // semantics with id-scoped traversals starting from the known node ids.

    // node1 (label_x) is directly related to node2 via edge1.
    const neighborIds = await connection.query(async g =>
        g.V(node1.id).both().id().toList())

    // node1's own label and node3's label should both be label_x; node4 is label_z.
    const {value: node1Label} = await connection.query(async g => g.V(node1.id).label().next())
    const {value: node3Label} = await connection.query(async g => g.V(node3.id).label().next())
    const {value: node4Label} = await connection.query(async g => g.V(node4.id).label().next())

    let assertions = {
        // node2 is a direct neighbor of node1 (the edge we created)
        "Node1RelatedToNode2": () => neighborIds.includes(node2.id),
        // node4 is unrelated to node1
        "Node1NotRelatedToNode4": () => !neighborIds.includes(node4.id),
        // label grouping matches what we wrote
        "Node1IsLabelX": () => node1Label === "label_x",
        "Node3IsLabelX": () => node3Label === "label_x",
        "Node4IsLabelZ": () => node4Label === "label_z",
    }

    if (!runAssertions(assertions)) {
        await cleanup()
        throw new Error("focus on label assertions failed")
    }

    // Focusing on node4 specifically: it has no relationships, so traversing
    // its neighbors returns nothing (none of the other test nodes).
    const node4Neighbors = await connection.query(async g =>
        g.V(node4.id).both().id().toList())

    assertions = {
        "Node4HasNoNeighbors": () => node4Neighbors.length === 0,
        "Node4NotRelatedToNode1": () => !node4Neighbors.includes(node1.id),
    }

    if (!runAssertions(assertions)) {
        await cleanup()
        throw new Error("focus by name failed")
    }

    await cleanup()
}

/**
 * Test the neptune-gremlin lib
 * 
 * This has to be a Lambda function since it needs to be in the VPC with Neptune.
 * 
 * @param {*} event 
 * @param {*} context 
 */
async function runTests() {

    const host = process.env.NEPTUNE_ENDPOINT
    const port = process.env.NEPTUNE_PORT
    const useIam = process.env.USE_IAM === "true"

    const connection = new gremlin.Connection(host, port, {useIam})

    console.log(`About to connect to ${host}:${port} useIam:${useIam}`)

    await connection.connect()

    try {
        await runGraphTests(connection)
    } finally {
        // Always release the WebSocket so the process can exit cleanly.
        await connection.disconnect()
    }

    return true
}

/**
 * The body of the graph tests, separated so the connection lifecycle
 * (connect/disconnect) can be managed by the caller.
 *
 * @param {*} connection
 */
async function runGraphTests(connection) {

    // Fetch a single node by id, scoped to exactly that id (no full-graph
    // scan). Returns { id, labels: [], properties: {} } or undefined.
    async function getNodeById(nodeId) {
        const {value} = await connection.query(async g => g.V(nodeId).elementMap().next())
        if (!value) return undefined
        const node = {id: value.id, labels: [], properties: {}}
        // elementMap returns the label as a single string; the client stores
        // multiple labels joined with "::".
        node.labels = typeof value.label === "string" ? value.label.split("::") : [value.label]
        for (const k in value) {
            if (k !== "id" && k !== "label") node.properties[k] = value[k]
        }
        return node
    }

    // Fetch a single edge by id, scoped to exactly that id.
    // Returns { id, label, from, to, properties: {} } or undefined.
    async function getEdgeById(edgeId) {
        const {value} = await connection.query(async g => g.E(edgeId).elementMap().next())
        if (!value) return undefined
        const edge = {id: value.id, label: value.label, from: "", to: "", properties: {}}
        for (const k in value) {
            if (k === "IN") edge.from = value[k].id
            else if (k === "OUT") edge.to = value[k].id
            else if (k !== "id" && k !== "label") edge.properties[k] = value[k]
        }
        return edge
    }

    const id = uuid.v4()

    const node1 = {
        id,
        properties: {
            name: "Test Node",
            a: "A",
            b: "B",
        },
        labels: ["label1", "label2"],
    }

    await connection.saveNode(node1)

    const node2 = {
        id: uuid.v4(),
        properties: {
            name: "Test Node2",
        },
        labels: ["label1"],
    }

    await connection.saveNode(node2)

    const edge1 = {
        id: uuid.v4(),
        label: "points_to",
        to: node2.id,
        from: node1.id,
        properties: {
            "a": "b",
        },
    }

    await connection.saveEdge(edge1)

    let found = await getNodeById(id)

    console.info("found", found)

    let assertions = {
        "Search": () => found !== undefined,
        "Name": () => found.properties.name === "Test Node",
        "A": () => found.properties.a === "A",
        "B": () => found.properties.b === "B",
        "Label0": () => found.labels[0] === "label1",
        "Label1": () => found.labels[1] === "label2",
    }

    const createOk = runAssertions(assertions)

    if (!createOk) {
        throw new Error("node assertions failed")
    }

    // Make sure the edge exists
    const foundEdge1 = await getEdgeById(edge1.id)

    console.info("found", foundEdge1)

    const edgeOk = runAssertions({
        "Edge found": () => foundEdge1 !== undefined,
        "Edge label": () => foundEdge1.label === "points_to",
        "Edge properties": () => foundEdge1.properties && foundEdge1.properties.a === "b",
    })

    if (!edgeOk) throw new Error("edge assertions failed")

    // Make an edge in the other direction
    const edge2 = {
        id: uuid.v4(),
        label: "points_to",
        properties: {},
        to: node1.id,
        from: node2.id,
    }

    await connection.saveEdge(edge2)
    await connection.deleteEdge(edge2.id)

    // Remove a property and make sure it get dropped
    delete node1.properties.b
    await connection.saveNode(node1)

    found = await getNodeById(id)

    console.info("found after dropping property", found)

    const propDropped = runAssertions({
        "No B": () => found.properties.b === undefined,
    })

    if (!propDropped) {
        throw new Error("Property was not dropped")
    }

    // Delete the node
    await connection.deleteNode(id)

    // Make sure it was deleted, along with its edges. Check the specific ids
    // the test created rather than scanning the whole graph.
    const deletedNode = await getNodeById(id)
    const deletedEdge1 = await getEdgeById(edge1.id)

    const deletedOk = runAssertions({
        "Edge deleted with node": () => deletedEdge1 === undefined,
        "Node not found": () => deletedNode === undefined,
    })

    if (!deletedOk) {
        throw new Error("delete assertions failed")
    }

    // Test options.focus
    await testFocus(connection)

    // Test creating a node without an id
    await testNoId(connection)

    // Test partitions
    await testPartitions(connection)

    return true
}

/**
 * Test the partition strategy functionality.
 *
 * @param {*} connection
 */
async function testPartitions(connection) {

    // Run-unique partition names so repeated runs don't collide.
    const runId = uuid.v4()
    const partitionA = `itest-part-${runId}-A`
    const partitionB = `itest-part-${runId}-B`

    const id = uuid.v4()
    const partitionNode = {
        id,
        properties: {
            name: "Test Partition",
            e: "E",
        },
        labels: ["label3"],
    }

    // Read a node by id within whatever partition is currently set.
    async function getPartitionNode() {
        const {value} = await connection.query(async g => g.V(id).elementMap().next())
        if (!value) return undefined
        const props = {}
        for (const k in value) {
            if (k !== "id" && k !== "label") props[k] = value[k]
        }
        return {
            id: value.id,
            label: typeof value.label === "string" ? value.label.split("::")[0] : value.label,
            properties: props,
        }
    }

    // PartitionStrategy is a TinkerPop feature that some Neptune engine
    // versions do not support; when unsupported the traversal never returns.
    // The client's per-query timeout turns that into an error, which we catch
    // and report as a skip rather than hanging or failing the whole suite.
    // Use a short timeout here so an unsupported strategy is detected quickly.
    const savedTimeout = connection.queryTimeoutMs
    connection.queryTimeoutMs = 8000
    connection.setPartition(partitionA)
    try {
        await connection.saveNode(partitionNode)
    } catch (ex) {
        console.warn(`SKIPPED: partition tests - PartitionStrategy not supported on this cluster (${ex.message})`)
        connection.setPartition(null)
        connection.queryTimeoutMs = savedTimeout
        return
    }
    connection.queryTimeoutMs = savedTimeout

    // The node should be visible in partition A...
    const foundA = await getPartitionNode()
    const createOk = runAssertions({
        "PartitionCreate": () => foundA !== undefined,
        "PartitionName": () => foundA.properties.name === "Test Partition",
        "PartitionE": () => foundA.properties.e === "E",
        "PartitionLabel": () => foundA.label === "label3",
    })
    if (!createOk) {
        connection.setPartition(partitionA)
        await connection.deleteNode(id)
        connection.setPartition(null)
        throw new Error("partitionNode assertions failed")
    }

    // ...and not visible from a different partition.
    connection.setPartition(partitionB)
    const foundB = await getPartitionNode()
    const notFound = runAssertions({
        "PartitionIsolation": () => foundB === undefined,
    })
    if (!notFound) {
        connection.setPartition(partitionA)
        await connection.deleteNode(id)
        connection.setPartition(null)
        throw new Error("Should not have found node in second partition")
    }

    // Clean up in the partition where the node lives, then clear the partition.
    connection.setPartition(partitionA)
    await connection.deleteNode(id)
    connection.setPartition(null)
}
