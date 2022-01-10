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

    let searchResult = await connection.search({})

    let found

    for (const node of searchResult.nodes) {
        if (node.id === id) {
            found = node
            break
        }
    }

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
    found = null

    for (const edge of searchResult.edges) {
        if (edge.id === edge1.id) {
            found = edge
            break
        }
    }

    console.info("found", found)

    const edgeOk = runAssertions({
        "Edge found": () => found != null,
        "Edge label": () => found.label === "points_to",
        "Edge properties": () => found.properties && found.properties.a === "b",
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

    searchResult = await connection.search({})

    found = undefined

    for (const node of searchResult.nodes) {
        if (node.id === id) {
            found = node
            break
        }
    }

    console.info("found after dropping property", found)

    const propDropped = runAssertions({
        "No B": () => found.properties.b === undefined,
    })

    if (!propDropped) {
        throw new Error("Property was not dropped")
    }

    // Delete the node
    await connection.deleteNode(id)

    // Make sure it was deleted, along with its edges
    searchResult = await connection.search({})

    const deletedOk = runAssertions({
        "Edges not found": () => {
            let foundEdge
            for (const edge of searchResult.edges) {
                if (edge.from === id || edge.to === id) {
                    foundEdge = edge
                    break
                }
            }
            return foundEdge === undefined
        },
        "Node not found": () => {
            let foundDeleted
            for (const node of searchResult.nodes) {
                if (node.id === id) {
                    foundDeleted = node
                    break
                }
            }
            return foundDeleted === undefined
        },
    })

    if (!deletedOk) {
        throw new Error("delete assertions failed")
    }

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

    // Set the partition
    connection.setPartition("test_partition")

    const id = uuid.v4()

    const partitionNode = {
        id,
        properties: {
            name: "Test Partition",
            e: "E",
        },
        labels: ["label3"],
    }

    await connection.saveNode(partitionNode)

    let searchResult = await connection.search({})

    let found

    for (const node of searchResult.nodes) {
        if (node.id === id) {
            found = node
            break
        }
    }

    console.info("found", found)

    let assertions = {
        "Search": () => found !== undefined,
        "Name": () => found.properties.name === "Test Partition",
        "E": () => found.properties.e === "E",
        "Label": () => found.labels[0] === "label3",
    }

    const createOk = runAssertions(assertions)

    if (!createOk) {
        throw new Error("partitionNode assertions failed")
    }

    // Change to a different partition
    connection.setPartition("second_partition")

    searchResult = await connection.search({})

    found = undefined

    for (const node of searchResult.nodes) {
        if (node.id === id) {
            found = node
            break
        }
    }

    console.info("found", found)

    assertions = {
        "Search": () => found === undefined,
    }

    const notFound = runAssertions(assertions)

    if (!notFound) {
        throw new Error("Should not have found node in second partition")
    }

    await connection.deleteNode(id)

}