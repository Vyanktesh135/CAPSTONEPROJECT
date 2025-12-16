import { Outlet } from "react-router";
import './app.css'

export default function app(){
  return (
    <html>
      <head>
        <title>Merch Lense</title>
      </head>
      <body>
          <Outlet></Outlet>
      </body>
    </html>
  )
}