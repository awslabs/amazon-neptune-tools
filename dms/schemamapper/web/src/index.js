import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import reportWebVitals from './reportWebVitals';
import { BrowserRouter as Router, Switch, Route, Link } from "react-router-dom";
import SignUpOrSignIn from './components/login'
import SchemaMapper from './components/schemamapper'
import Home from './components/home'
import 'bootstrap/dist/css/bootstrap.min.css';

ReactDOM.render(
  <Router>
    <Router>
      <div>
        <Route exact path="/">
          <SchemaMapper />
          {/* <Schemas/> */}
        </Route>
        {/* <Route path="/sm">
          <SchemaMapper />
        </Route> */}
        <Route path="/login">
          <SignUpOrSignIn />
        </Route>
      </div>
    </Router>
  </Router >,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
