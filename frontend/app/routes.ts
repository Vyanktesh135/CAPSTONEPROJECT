import { type RouteConfig, index,route } from "@react-router/dev/routes";

export default [
    route("/","routes/home.tsx"),
    route("/upload_file","routes/upload.tsx")
] satisfies RouteConfig;
