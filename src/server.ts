import App from "@/app";
import IndexRoute from "@routes/index.route";
import KvsRoute from "@routes/kvs.route";
import ViewRoute from "@/routes/view.route";
import validateEnv from "@utils/validateEnv";

validateEnv();

const app = new App([new IndexRoute(), new KvsRoute(), new ViewRoute()]);

app.listen();
