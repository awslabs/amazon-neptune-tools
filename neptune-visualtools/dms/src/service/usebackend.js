import { useState, useEffect, useReducer } from "react";
import { useConfiguration } from './useconfiguration'
import Axios from 'axios'
import Amplify, { Auth } from 'aws-amplify';

export function useBackend() {
    // const [backend, setBackend] = useState([]);
    const { API } = useConfiguration();
    const initBackend = async () => {

        const Backend = await Axios.get(API + "/backend").then((response) => {
            console.log(response);
            return response.data;
        })

        return Backend;
    };

    const saveConfiguration = async (configId) => {
        const user = await Auth.currentAuthenticatedUser();
        if (user !== undefined) {
            var configurlUrl = `${API}/configuration/${configId}`;
            console.log(configurlUrl);

            var result = await Axios.post(configurlUrl, JSON.stringify({
                data: {
                    configId: configId,
                    by: user?.username
                }
            })).then((response) => {
                console.log(response);
                return true;
            }, (error) => {
                console.log(error);
                return false;
            });

            return result;
        }
    }

    return {
        saveConfiguration,
        initBackend
    };
}