import { HTTPCodeEnum, HTTPMethodEnum } from "@core/enums/core.enum";
import type { IHTTPdetail, IHttpOptions, IHTTPresponse } from "@core/interfaces/core.interface";
import { HttpRequest } from "@core/utils/http";
import { useEffect, useState } from "react"


export default function WithAuth(Component: any){

    return function Auth(){

        const url = "http://localhost:8000/api/v1/auth/verify/";

        const [authenticated, setAuthenticated] = useState<boolean>(false);

        useEffect(()=>{
            authenticate();
        },[]);


        const authenticate = async () => {

            const options: IHttpOptions = HttpRequest.generateOptions(url, {});
            const response: IHTTPresponse<IHTTPdetail> = await HttpRequest.request<IHTTPdetail>(HTTPMethodEnum.GET, options, true);

            if(response.status != HTTPCodeEnum.OK){
                window.location.replace("/administrator/login");
                return;
            }
            
            setAuthenticated(true);

        }

        if(authenticated){
            return <Component/>;
        }

        return null;

    }
}