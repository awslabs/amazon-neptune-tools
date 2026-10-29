import Layout, { siteTitle } from './layout'
import { Form, FormControl, Button, Card, Container, Row, Col, ButtonToolbar } from 'react-bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';
import { Link } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { useHistory, useLocation, useParams } from "react-router-dom";
import { v4 as uuidv4 } from 'uuid';
// import FileUpload from './fileupload';
import Connections from './connections'


function SchemaMapper() {

  const history = useHistory();
  const [connections, setConnections] = useState([]);
  const [pageRefresh, setPageReferesh] = useState(false);
  const [showNewVertex, setShowNewVertex] = useState(false);
  const [showNewEdge, setShowNewEdge] = useState(false);
  const [tableList, setTableList] = useState([]);
  const [columnList, setColumnList] = useState([]);
  const [vertices, setVertices] = useState([]);
  const [edges, setEdges] = useState([]);
  const [currentVertex, setCurrentVertex] = useState({});
  const [currentEdge, setCurrentEdge] = useState({});
  const [vertexdefinitions, setVertexdefinitions] = useState([]);
  const [currentVertexIDTemplate, setcurrentVertexIDTemplate] = useState("");
  const [currentEdgeV1Template, setCurrentEdgeV1Template] = useState("[from_vertex]");
  const [currentEdgeV2Template, setCurrentEdgeV2Template] = useState("[to_vertex]");
  const [currentEdgeV1Definition, setCurrentEdgeV1Definition] = useState("");
  const [currentEdgeV2Definition, setCurrentEdgeV2Definition] = useState("");
  const [finalMapping, setFinalMapping] = useState({ "rules": [] });


  useEffect(() => {
    var connectionsNew = [
      {
        id: "37dabab5-1693-4bbc-bbc3-1a4abdb93f76",
        title: "Connection2",
        type: "MySQL",
        description: "Connection1",
        sqlEndpoint: "ecommerce.cy3md3b35fci.us-west-2.rds.amazonaws.com",
        sqlPort: 3306
      }
    ];

    setConnections(connectionsNew);

  }, [pageRefresh]);

  var con = null;

  function loadSchemas(e) {
  }


  const [state, setState] = useState({
    selectedFile: null,
    root: null
  });

  function csvToArray(str, delimiter = ",") {
    // slice from start of text to the first \n index
    // use split to create an array from string by delimiter

    str = str.replace("\r", "");
    const headers = str.slice(0, str.indexOf("\n")).split(delimiter);

    // slice from \n index + 1 to the end of the text
    // use split to create an array of each csv value row
    const rows = str.slice(str.indexOf("\n") + 1).split("\n");

    // Map the rows
    // split values from each row into an array
    // use headers.reduce to create an object
    // object properties derived from headers:values
    // the object passed as an element of the array
    const arr = rows.map(function (row) {
      const values = row.split(delimiter);
      const el = headers.reduce(function (object, header, index) {
        object[header] = values[index];
        return object;
      }, {});
      return el;
    });

    // return the array
    return arr;
  }


  function arrayToTree(array) {
    var root = {
      tables: []
    };

    array.forEach(element => {
      if (element.TABLE_NAME !== undefined && element.TABLE_NAME !== "")
        var refObject = null;
      refObject = root.tables.find(table => table.name.toLowerCase() === element.TABLE_NAME.toLowerCase());

      if (refObject === undefined) {
        root.tables.push({
          "name": element.TABLE_NAME
        })

        refObject = root.tables.find(table => table.name.toLowerCase() === element.TABLE_NAME.toLowerCase());
        refObject.columns = [];
      }

      if (element.COLUMN_NAME !== undefined && element.COLUMN_NAME !== "") {
        refObject.columns.push(
          {
            name: element.COLUMN_NAME,
            type: element.COLUMN_TYPE,
            isnullable: element.IS_NULLABLE,
            ordinal: element.ORDINAL_POSITION
          }
        )
      }
    });

    console.log(root);
    return root;
  }

  // On file select (from the pop up)
  function onFileChange(e) {
    // Update the state
    setState({ selectedFile: e.target.files[0] });
  };

  // On file upload (click the upload button)
  // function  onFileChange() {

  //     // Create an object of formData
  //     const formData = new FormData();

  //     // Update the formData object
  //     formData.append(
  //         "myFile",
  //         state.selectedFile,
  //         state.selectedFile.name
  //     );

  //     // Details of the uploaded file
  //     console.log(state.selectedFile);

  //     // Request made to the backend api
  //     // Send formData object
  //     axios.post("api/uploadfile", formData);
  // };

  // File content to be displayed after
  // file upload is complete
  function fileData() {

    if (state.selectedFile) {

      const reader = new FileReader();
      reader.readAsText(state.selectedFile);
      reader.onload = function (event) {
        console.log(event.target.result); // the CSV content as string
        //alert(event.target.result);
        var resultArray = csvToArray(event.target.result);

        console.log(resultArray);
        //alert(JSON.stringify(resultArray));

        var resultTree = arrayToTree(resultArray);
        console.log(resultTree)
        //alert(JSON.stringify(resultTree));
        setState({
          root: resultTree
        });

        setTableList(resultTree.tables);
      };

      return (
        <div>
          <h2>File Details:</h2>
          <p>File Name: {state.selectedFile.name}</p>
          <p>File Type: {state.selectedFile.type}</p>
        </div>
      );
    }
    // else {
    //     return (
    //         <div>
    //             <br />
    //             <h4>Choose before Pressing the Upload button</h4>
    //         </div>
    //     );
    // }
  };

  function cancelAddNewVertex() {
    setCurrentVertex({});
    setShowNewVertex(false);
  }

  function SaveNewVertexMapping(e) {
    var vertexlabel = document.getElementById("formVLabel").value;
    var formVTables = document.getElementById("formVTables");
    var vertextable = formVTables.options[formVTables.selectedIndex].text;
    var propertyMappings = [];

    if (currentVertex.attributes !== undefined) {
      currentVertex.attributes.forEach((item, index) => {
        var propertyValueTemplate = document.getElementById(currentVertex.id + "-propertyValueTemplate-" + index);
        var vertexpropertyTemplate = propertyValueTemplate.options[propertyValueTemplate.selectedIndex].text;
        var vertexpropertyType = propertyValueTemplate.options[propertyValueTemplate.selectedIndex].attributes["type"].value;

        var vertexpropertyName = document.getElementById(currentVertex.id + "-propertyName-" + index).value;

        propertyMappings.push({
          property_value_template: "{property_vale_template}".replace("property_vale_template", vertexpropertyTemplate),
          property_name: vertexpropertyName,
          property_value_type: getGraphPropertyType(vertexpropertyType)
        });

      })
    }

    // var propertyId = propertyMappings.find(property => (property.property_name === "id"));

    if (currentVertexIDTemplate === undefined) {
      alert("Vertex Id template not defined");
      return;
    }

    var nv = {
      "rule_id": uuidv4(),
      "rule_name": "vertex_mapping_rule_from_nodes" + uuidv4(),
      "table_name": vertextable,
      "vertex_definitions": [
        {
          "vertex_id_template": "{vertex_id_template}".replace("vertex_id_template", currentVertexIDTemplate),
          "vertex_label": vertexlabel,
          "vertex_definition_id": uuidv4()
        }
      ]
    };

    if (propertyMappings.length > 0) {
      nv.vertex_definitions[0].vertex_properties = propertyMappings;
    }

    setVertices((vertices) => {
      var newvertices = [nv, ...vertices]
      return newvertices;
    });

    setVertexdefinitions((vertexdefinitions) => {
      var newvertexdefinitions = nv.vertex_definitions.concat(vertexdefinitions);
      return newvertexdefinitions;
    });

    setFinalMapping((finalMapping) => {
      finalMapping.rules.push(nv);

      return finalMapping;
    });

    console.log(nv.vertex_definitions);
    console.log(vertexdefinitions);

    setCurrentVertex({});
    setShowNewVertex(false);
  }

  function cancelAddNewEdge() {
    setCurrentEdge({});
    setShowNewEdge(0);

    setCurrentEdgeV1Template("[from_vertex]")
    setCurrentEdgeV2Template("[to_vertex]");
    setCurrentEdgeV1Definition("");
    setCurrentEdgeV2Definition("");
  }

  function SaveNewEdgeMapping(e) {

    var edgelabel = document.getElementById("formELabel").value;
    var formVTables = document.getElementById("formETables");
    var edgetable = formVTables.options[formVTables.selectedIndex].text;
    var propertyMappings = [];

    if (currentEdge.attributes !== undefined) {
      currentEdge.attributes.forEach((item, index) => {
        var propertyValueTemplate = document.getElementById(currentEdge.id + "-propertyValueTemplate-" + index);
        var edgepropertyTemplate = propertyValueTemplate.options[propertyValueTemplate.selectedIndex].text;
        var edgepropertyType = propertyValueTemplate.options[propertyValueTemplate.selectedIndex].attributes["type"].value;

        var vertexpropertyName = document.getElementById(currentEdge.id + "-attributepropertyName-" + index).value;

        propertyMappings.push({
          property_value_template: "{property_value_template}".replace("property_value_template", edgepropertyTemplate),
          property_name: vertexpropertyName,
          property_value_type: getGraphPropertyType(edgepropertyType)
        });

      })
    }

    // var propertyId = propertyMappings.find(property => (property.property_name === "id"));

    // if (propertyId === undefined) {
    //     alert("Id attribute not defined on node/edge");
    //     return;
    // }

    var ne = {
      "rule_id": uuidv4(),
      "rule_name": "edge_mapping_rule" + uuidv4(),
      "table_name": edgetable,
      "edge_definitions": [
        {
          "from_vertex": {
            "vertex_id_template": currentEdgeV1Template,
            "vertex_definition_id": currentEdgeV1Definition,
          },
          "to_vertex": {
            "vertex_id_template": currentEdgeV2Template,
            "vertex_definition_id": currentEdgeV2Definition,
          },
          "edge_id_template": {
            "label": edgelabel,
            "template": currentEdgeV1Template + "-" + currentEdgeV2Template
          }
        }
      ]
    };

    if (propertyMappings.length > 0) {
      ne.edge_definitions.edge_properties = propertyMappings;
    }

    setEdges((edges) => {
      var newedges = [ne, ...edges]
      return newedges;
    });

    setFinalMapping((finalMapping) => {
      finalMapping.rules.push(ne);
      return finalMapping;
    });

    setCurrentEdge({});
    setShowNewEdge(false);
  }

  function getGraphPropertyType(sqltype) {

    if (sqltype.indexOf("varchar") !== -1) {
      sqltype = "varchar";
    }

    // MSSql Specific data type
    if (sqltype.indexOf("hierarchyid") !== -1) {
      sqltype = "varchar";
    }


    switch (sqltype) {
      case "smallint":
        return "Int";
      case "bigint":
        return "Long";
      case "varchar":
        return "String";
      case "text":
        return "String";
      default:
        return "String";
    }
  }

  function addNewVertexAttribute(e, vertexid) {
    var vertex = JSON.parse(JSON.stringify(currentVertex));

    if (vertex.attributes === undefined)
      vertex.attributes = []

    vertex.attributes.push({ id: uuidv4(), key: "", value: "" })

    setCurrentVertex(vertex);
  }

  function cancelNewVertexAttribute(e, vertexid, vertexAttributeId) {
    alert(vertexAttributeId);

    var vertex = JSON.parse(JSON.stringify(currentVertex));
    vertex.attributes = vertex.attributes.filter(attribute => attribute.id !== vertexAttributeId);

    setCurrentVertex(vertex);
  }

  function setAttributeValue(e, vertexid, vertexAttributeId) {

  }

  function setAttributeKey(e, vertexid, vertexAttributeId) {

  }

  function addNewEdgeAttribute(e, vertexid) {
    var edge = JSON.parse(JSON.stringify(currentEdge));

    if (edge.attributes === undefined)
      edge.attributes = []


    edge.attributes.push({ key: "", value: "" })

    setCurrentEdge(edge);
  }

  function addNewEdge(e) {
    setCurrentEdge({
      "id": uuidv4(),
      "label": "",
      "mandatoryattributes": [
        { key: "from_vertex", value: "" },
        { key: "to_vertex", value: "" },
        { key: "edge_id_template", value: "" }
      ]
    })
    setShowNewEdge(true);
    setShowNewVertex(false);
  }

  function addNewVertex(e) {
    setCurrentVertex({
      "id": uuidv4(),
      "label": "",
      "mandatoryattributes": [
        { key: "vertex_id_template", value: "" }
      ]
    })
    setShowNewVertex(true);
    setShowNewEdge(false);
  }

  function updateColumList(e) {
    var formVTables = document.getElementById("formVTables");
    var tableName = formVTables.options[formVTables.selectedIndex].text;

    var columns = state.root.tables.find(table => (table.name === tableName)).columns;
    setColumnList(columns);
  }

  function updateColumListE(e) {
    var formETables = document.getElementById("formETables");
    var tableName = formETables.options[formETables.selectedIndex].text;

    var columns = state.root.tables.find(table => (table.name === tableName)).columns;
    setColumnList(columns);
  }

  function setVertexTemplate(e, attributekey, param1, param2, param3) {

    //-vertexdefinition-
    //-vertexidtemplate-
    var element = document.getElementById(param1 + param2 + param3);
    var elementvalue = element.options[element.selectedIndex].value;

    //  alert(elementvalue);
    setcurrentVertexIDTemplate(elementvalue);
  }

  function setEdgeTemplate(e, attributekey, param1, param2, param3) {

    //-vertexdefinition-
    //-vertexidtemplate-
    var element = document.getElementById(param1 + param2 + param3);
    var elementvalue = element.options[element.selectedIndex].value;

    if (attributekey === "from_vertex") {
      if (param2 === "-vertexdefinition-") {
        setCurrentEdgeV1Definition(elementvalue)
      }
      else {
        setCurrentEdgeV1Template("{template}".replace("template", elementvalue))
      }
    }

    if (attributekey === "to_vertex") {
      if (param2 === "-vertexdefinition-") {
        setCurrentEdgeV2Definition(elementvalue)

      } else {
        setCurrentEdgeV2Template("{template}".replace("template", elementvalue))
      }
    }

  }


  function copyTemplateToClipboard() {
    var copyText = document.getElementById("preTemplate");
    //copyText.select();
    //copyText.setSelectionRange(0, 99999); /* For mobile devices */
    navigator.clipboard.writeText(copyText.innerText);
    alert("Copied to clipboard");
  }

  return (

    <Layout schemamapper>
      <Row>
        <Col>
          <div>
            <br/>
            <blockquote>Use this utility to create Graph Schema Mapping for Property Graph in Amazon Neptune, used by AWS Database Migration Service to migrate data from RDBMS systems to Amazon Neptune.
              {/* The schema mapping json is needed by AWS Database Migration Service
              to create csv ready to be imported inside Amazon Neptune. */}
            </blockquote>
          </div>
          <div>
            <Row>
              <Col>
                <Row>
                  <Col md={8}>
                    <div>
                      Upload Schema for your database tables as csv <a href="/sample_sql_schema.csv">(click to download mysql schema sample) </a>
                    </div>
                  </Col>
                  <Col md={4}>
                    <div>
                      <input className="primary" type="file" onChange={e => onFileChange(e)} />
                    </div>
                    {fileData()}
                  </Col>
                </Row>
                <hr />
                <Row>
                  <Col>Sql Query to generate schema:</Col>
                </Row>
                <Row>
                  <Col> <div className='text-muted'> <b>MySql</b> :  SELECT
                    TABLE_NAME,
                    COLUMN_NAME,
                    ORDINAL_POSITION,
                    IS_NULLABLE,
                    COLUMN_TYPE
                    FROM
                    information_schema.columns
                    WHERE
                    table_schema = &lt;DBName&gt;</div>
                  </Col>
                </Row>
                <Row>
                  <Col>
                  <div className='text-muted'>
                  <b>MSSql</b> : SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE + CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' ELSE '' END AS COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = &lt;DBName&gt;</div>
                  </Col>
                </Row>
                <hr />
                <Row>
                  <Col md={3}>
                    <Row>
                      <Col>RDBMS tables</Col>
                    </Row>
                    <Row>
                      <Col>
                        <Card>
                          <Card.Title>
                            {/* SQL Schema */}
                          </Card.Title>
                          <Card.Body>
                            {state.root?.tables?.map((table, index) => (
                              <div>
                                <h5>{table.name}</h5>
                                <div>
                                  {table.columns?.map((column, index) => (
                                    <div className="tree-depth-1">
                                      <div><b>{column.name}</b>, {column.type}, {column.isnullable}</div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            ))}
                          </Card.Body>
                        </Card>
                      </Col>
                    </Row>
                    <Row>
                      {/* <Connections></Connections> */}
                    </Row>
                  </Col>
                  <Col>
                    <Row>
                      <Col>
                        Graph Mappings
                      </Col>
                    </Row>
                    <Row>
                      <Col>
                        <Card>
                          <Card.Title>
                            {/* Graph Mappings */}
                          </Card.Title>
                          <Card.Body>
                            <div className="alignRight">
                              <Button variant="primary" onClick={e => addNewVertex(e)}>Add Vertex</Button> &nbsp; <Button variant="primary" onClick={e => addNewEdge(e)}>Add Edge</Button>
                            </div>

                            <div>
                              {showNewVertex > 0 &&
                                <Card>
                                  <Card.Title>
                                    New Vertex
                                  </Card.Title>
                                  <Card.Body>
                                    <div className="NCForm">
                                      <Form>
                                        <Form.Group className="mb-3" controlId="formVLabel">
                                          <Form.Label>Vertex label</Form.Label>
                                          <Form.Control type="text" placeholder="Enter title" />
                                          {/* <Form.Text className="text-muted">
                                                                        Enter Label as per graph model
                                                                    </Form.Text> */}
                                        </Form.Group>
                                        <Form.Group className="mb-1" controlId="formVTables">
                                          <Form.Label>Table to refer</Form.Label>
                                          <Form.Select aria-label="Default select example" onChange={e => updateColumList(e)}>
                                            <option>Select schema</option>
                                            {tableList.map((table, index) => (
                                              <option>{table.name}</option>
                                            ))}
                                          </Form.Select>

                                          {/* <Form.Text className="text-muted">
                                                                        Select table_name
                                                                    </Form.Text> */}
                                        </Form.Group>


                                        {currentVertex?.mandatoryattributes?.map((attribute, index) => (
                                          <Row>
                                            <Col>
                                              {/* <Form.Control ></Form.Control> */}
                                              <label id={currentVertex?.id + "-propertyName1-" + index} type="label">{attribute?.key}</label>
                                            </Col>
                                            <Col>
                                              {/* {attribute?.key !== "edge_id_template" && */}
                                              <Form.Group className="mb-1" controlId={currentVertex?.id + "-vertexidtemplate-" + index} onChange={e => setVertexTemplate(e, attribute?.key, currentVertex?.id, "-vertexidtemplate-", index)}>
                                                <Form.Select aria-label="Default select example">
                                                  <option>Select table column</option>
                                                  {columnList?.map((column, columnIndex) => (
                                                    <option type={column.type} value={column.name}>{column.name}</option>
                                                  ))}
                                                </Form.Select>
                                              </Form.Group>
                                              {/* } */}
                                            </Col>
                                          </Row>
                                        ))}
                                        <hr />
                                        {currentVertex !== undefined &&
                                          <Row class="rightAlign">
                                            <Col md="10"></Col>
                                            <Col md="2">
                                              <Button slot="end" variant="primary" onClick={e => addNewVertexAttribute(e, currentVertex?.id)}>add attribute</Button>
                                            </Col>
                                          </Row>
                                        }
                                        <hr />
                                        {currentVertex?.attributes?.map((attribute, index) => (
                                          <Row>
                                            <Col>
                                              <Form.Group className="mb-1" controlId={currentVertex?.id + "-propertyValueTemplate-" + index}>
                                                <Form.Text className="text-muted">
                                                  Select table Column
                                                </Form.Text>
                                                <Form.Select aria-label="Default select example" onChange={e => setAttributeKey(e, attribute?.id)}>
                                                  <option>Select Column</option>
                                                  {columnList?.map((column, columnIndex) => (
                                                    <option type={column.type}>{column.name}</option>
                                                  ))}
                                                </Form.Select>

                                              </Form.Group>
                                            </Col>

                                            <Col>
                                              <Form.Group className="mb-1" >
                                                <Form.Text className="text-muted">
                                                  Enter Vertex Property Name
                                                </Form.Text>
                                                <Form.Control id={currentVertex?.id + "-propertyName-" + index} type="text" placeholder="Enter title" onChange={e => setAttributeValue(e, attribute?.id)}></Form.Control>
                                              </Form.Group>
                                            </Col>
                                            <Col>
                                              <br />
                                              {/* <Button slot="end" variant="primary" size="sm" onClick={e => cancelNewVertexAttribute(e, currentVertex?.id, attribute?.id)}>cancel</Button> */}
                                            </Col>
                                          </Row>
                                        ))}
                                        <Button variant="primary" type="button" onClick={e => SaveNewVertexMapping(e)}>
                                          Save
                                        </Button>
                                        &nbsp;
                                        <Button variant="primary" type="button" onClick={e => cancelAddNewVertex(e)}>
                                          Cancel
                                        </Button>
                                      </Form>
                                    </div>
                                  </Card.Body>
                                </Card>
                              }

                              {showNewEdge > 0 &&
                                <Card>
                                  <Card.Title>
                                    New Edge
                                  </Card.Title>
                                  <Card.Body>
                                    <div className="NCForm">
                                      <Form>
                                        <Form.Group className="mb-3" controlId="formELabel">
                                          <Form.Label>Edge Label</Form.Label>
                                          <Form.Control type="text" placeholder="Enter title" />
                                          {/* <Form.Text className="text-muted">
                                                                        Enter Label as per graph model
                                                                    </Form.Text> */}
                                        </Form.Group>
                                        <Form.Group className="mb-1" controlId="formETables">
                                          <Form.Label>Table to refer</Form.Label>
                                          <Form.Select aria-label="Default select example" onChange={e => updateColumListE(e)}>
                                            <option>Select table</option>
                                            {tableList.map((table, index) => (
                                              <option>{table.name}</option>
                                            ))}
                                          </Form.Select>

                                          {/* <Form.Text className="text-muted">
                                                                        Select Table Or Vertex
                                                                    </Form.Text> */}

                                        </Form.Group>
                                        <br />

                                        {currentEdge?.mandatoryattributes?.map((attribute, index) => (
                                          <Row>
                                            <Col>
                                              {/* <Form.Control ></Form.Control> */}
                                              <label id={currentEdge?.id + "-propertyName-" + index} type="label">{attribute?.key}</label>
                                            </Col>
                                            <Col>
                                              {attribute?.key === "edge_id_template" &&

                                                // <Form.Text className="text-muted">

                                                // </Form.Text>
                                                <div>
                                                  <b>{currentEdgeV1Template}-{currentEdgeV2Template}</b>
                                                  {/* <br />
                                                                                    <h3>{currentEdgeV1Definition}-{currentEdgeV2Definition}</h3> */}
                                                </div>
                                              }
                                              {attribute?.key !== "edge_id_template" &&
                                                <Form.Group className="mb-1" controlId={currentEdge?.id + "-vertexdefinition-" + index} onChange={e => setEdgeTemplate(e, attribute?.key, currentEdge?.id, "-vertexdefinition-", index)}>
                                                  <Form.Select aria-label="Default select example">
                                                    <option>Select vertex definition</option>
                                                    {vertexdefinitions?.map((column, columnIndex) => (
                                                      <option value={column.vertex_definition_id}>{"VertexDefinition-" + column.vertex_definition_id}-{column.vertex_id_template}-{column.vertex_label}</option>
                                                    ))}
                                                  </Form.Select>
                                                </Form.Group>
                                              }
                                              {/* <Form.Text className="text-muted">
                                                                                Select Column Or Vertex Attribute
                                                                            </Form.Text> */}
                                              {/* } */}
                                            </Col>
                                            <Col>
                                              {/* <Row>
                                                                                <Col>
                                                                                    <Form.Text className="text-muted">
                                                                                        Select table column 
                                                                                    </Form.Text>
                                                                                </Col>
                                                                            </Row> */}
                                              <Row>
                                                <Col>

                                                  <div>
                                                    {attribute?.key !== "edge_id_template" &&
                                                      <Form.Group className="mb-1" controlId={currentEdge?.id + "-vertexidtemplate-" + index} onChange={e => setEdgeTemplate(e, attribute?.key, currentEdge?.id, "-vertexidtemplate-", index)}>
                                                        <Form.Select aria-label="Default select example">
                                                          <option>Select table column</option>
                                                          {columnList?.map((column, columnIndex) => (
                                                            <option type={column.type} value={column.name}>{column.name}</option>
                                                          ))}
                                                        </Form.Select>
                                                      </Form.Group>
                                                    }
                                                  </div>
                                                </Col>
                                              </Row>
                                            </Col>
                                          </Row>
                                        ))}

                                        <br />

                                        {currentEdge !== undefined &&

                                          <Row class="rightAlign">
                                            <Col md="10"></Col>
                                            <Col md="2">
                                              <Button slot="end" variant="primary" onClick={e => addNewEdgeAttribute(e, currentEdge?.id)}>add attribute</Button>
                                            </Col>
                                          </Row>
                                        }
                                        <hr />

                                        {currentEdge?.attributes?.map((attribute, index) => (
                                          <Row>

                                            <Col>
                                              <Form.Group className="mb-1" controlId={currentEdge?.id + "-propertyValueTemplate-" + index}>
                                                <Form.Select aria-label="Default select example">
                                                  <option>Select Column</option>
                                                  {columnList?.map((column, columnIndex) => (
                                                    <option type={column.type}>{column.name}</option>
                                                  ))}
                                                </Form.Select>

                                              </Form.Group>
                                            </Col>
                                            <Col>
                                              <Form.Control id={currentEdge?.id + "-attributepropertyName-" + index} type="text" placeholder="enter property name"></Form.Control>
                                            </Col>
                                          </Row>
                                        ))}
                                        <Button variant="primary" type="button" onClick={e => SaveNewEdgeMapping(e)}>
                                          Save
                                        </Button>
                                        &nbsp;
                                        <Button variant="primary" type="button" onClick={e => cancelAddNewEdge(e)}>
                                          Cancel
                                        </Button>
                                      </Form>
                                    </div>
                                  </Card.Body>
                                </Card>
                              }
                            </div>
                          </Card.Body>
                        </Card>
                      </Col>
                    </Row>
                    <Row>
                      <Col>
                        Generated Graph Template <Button size="sm" onClick={e => copyTemplateToClipboard(e)}>copy</Button>
                      </Col>
                    </Row>
                    <Row>
                      <Col>
                        <Card>
                          <Card.Title>
                            {/* Graph Template */}
                          </Card.Title>
                          <Card.Body>
                            {/* <textarea class="ace_text-input" id="myTextarea" width="100%" autocorrect="off" autocapitalize="none" spellcheck="false" wrap="off"></textarea> */}
                            <pre id="preTemplate">{JSON.stringify(finalMapping, null, 2)} </pre>
                          </Card.Body>
                        </Card>
                      </Col>
                    </Row>
                  </Col>
                </Row>
              </Col>
            </Row>
          </div>
        </Col>
      </Row>
    </Layout>
  );
}

export default SchemaMapper;
