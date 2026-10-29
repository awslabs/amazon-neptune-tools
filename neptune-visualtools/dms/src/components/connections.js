import Layout, { siteTitle } from './layout'
import {
    Navbar, Nav, NavDropdown, Form, FormControl, Button, Card, Container
    , Row, Col, ListGroup, Dropdown, FloatingLabel, Link, Image
} from 'react-bootstrap';
import React, { useState, useEffect } from 'react';
import { v4 as uuidv4 } from 'uuid';
import {
    BrowserRouter as Router,
    Switch,
    Route,
    // Link,
    Redirect,
    useHistory,
    useLocation
} from "react-router-dom";
import { sectionFooterSecondaryContent } from '@aws-amplify/ui';

function Connections() {

    const history = useHistory();
    const [connections, setConnections] = useState([]);
    const [pageRefresh, setPageReferesh] = useState(false);

    useEffect(() => {
        // initVacations().then(res => {
        //   //console.log(res.length);
        //   setVacation(vacations => {
        //     return res;
        //   });
        // });
        var connectionsNew = [
            {
                id: "37dabab5-1693-4bbc-bbc3-1a4abdb93f76",
                title: "Connection2",
                type: "MySQL",
                description: "Connection1",
                sqlEndpoint: "ecommerce.cy3md3b35fci.us-west-2.rds.amazonaws.com",
                sqlPort: 3306
            },
            {
                id: "f02636fb-9c9f-4e5d-a461-79b56ffcfc3c",
                title: "Connection2",
                type: "MySQL",
                description: "Connection3",
                sqlEndpoint: "ecommerce.cy3md3b35fci.us-west-2.rds.amazonaws.com",
                sqlPort: 3306
            },
            {
                id: "032a1c25-d5fc-40f8-ba6f-9aeec336a46e",
                title: "Connection3",
                type: "MySQL",
                description: "Connection3",
                sqlEndpoint: "ecommerce.cy3md3b35fci.us-west-2.rds.amazonaws.com",
                sqlPort: 3306
            }
        ];

        setConnections(connectionsNew);

    }, [pageRefresh]);



    var reviews = [
        {
            userprofile: "https://image.flaticon.com/icons/png/512/2922/2922518.png",
            comment: " Loved it, must visit for everyone"
        },
        {
            userprofile: "https://image.flaticon.com/icons/png/512/2922/2922579.png",
            comment: "A week well spent"
        }
    ];

    function redirectToConnection(e) {
        e.preventDefault();
        var connectionId = e.currentTarget.attributes['connectionid'].value
        history.push('/connections/' + connectionId);
    }

    function showDetails(e) {
        window.location.href = "/"
    }

    function captureOnChange(e, element) {

    }

    // useEffect(() => {
    //   alert(connections);
    // }, [connections]);


    function addNewConnnection(e) {
        var title = document.getElementById("formNCTitle").value;
        var endpoint = document.getElementById("formNCEndpoint").value;
        var port = document.getElementById("formNCPort").value;

        var formNCType = document.getElementById("formNCType");
        var type = formNCType.options[formNCType.value].text;

        var newConnection = {
            id: uuidv4(),
            title: title,
            type: type,
            sqlEndpoint: endpoint,
            sqlPort: port
        };

        const updatedConnections = [newConnection, ...connections]
        setConnections(updatedConnections);

        // alert(updatedConnections.length)
        // alert(connections.length)
    }

    return (
        <Row>
            <Col>
                {/* <Row>
              <Col>
                <div className="rightAlign">
                  <Button variant="primary">Add new connection</Button>
                </div>
              </Col>
            </Row> */}
                <Row>
                    <Col>
                        <Form>
                            <Form.Group className="mb-3" controlId="formNCTitle">
                                <Form.Label>Title</Form.Label>
                                <Form.Control type="text" placeholder="Enter title" />
                                {/* <Form.Text className="text-muted">
                        We'll never share your endpoint with anyone else.
                      </Form.Text> */}
                            </Form.Group>
                            <Form.Group className="mb-3" controlId="formNCEndpoint">
                                <Form.Label>Endpoint</Form.Label>
                                <Form.Control type="text" placeholder="Enter endpoint" />
                                {/* <Form.Text className="text-muted">
                        We'll never share your endpoint with anyone else.
                      </Form.Text> */}
                            </Form.Group>
                            <Form.Group className="mb-3" controlId="formNCPort">
                                <Form.Label>Port</Form.Label>
                                <Form.Control type="text" placeholder="Enter Port" />
                            </Form.Group>
                            {/* <Form.Group className="mb-3" controlId="formBasicCheckbox">
                      <Form.Check type="checkbox" label="Check me out" />
                    </Form.Group> */}
                            <Form.Group className="mb-1" controlId="formNCType">
                                <Form.Label>Type</Form.Label>
                                <Form.Select aria-label="Default select example">
                                    <option>Select Type</option>
                                    <option value="1">MySql</option>
                                    <option value="2">MariaDB</option>
                                </Form.Select>
                            </Form.Group>
                            <Button variant="primary" type="button" onClick={e => addNewConnnection(e)}>
                                Add new connection
                            </Button>
                        </Form>
                    </Col>
                </Row>
                {connections.map((connection, index) => (
                    <Row>
                        <Col>
                            <Card className="card">
                                {/* <Card.Img variant="top" className="cardImage" src={connection.banner} /> */}
                                <Card.Body>
                                    <Card.Title>{connection.title} | {connection.type}</Card.Title>
                                    <Card.Text>
                                        Endpoint: {connection.sqlEndpoint}
                                    </Card.Text>
                                    <Card.Text>
                                        Port: {connection.sqlPort}
                                    </Card.Text>
                                    <Button variant="outline-secondary" connectionId={connection.id} onClick={e => redirectToConnection(e, connection.id)}>View Schemas</Button>
                                </Card.Body>
                            </Card>
                        </Col>
                    </Row>
                ))}
            </Col>
        </Row>
    )
}

export default Connections