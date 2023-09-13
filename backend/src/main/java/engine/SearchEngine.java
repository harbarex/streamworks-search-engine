package engine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import engine.database.EngineBdb;
import engine.database.EngineBdbViews;
import engine.entity.Feature;
import engine.filters.AdminFilter;
import engine.filters.TokenFilter;
import engine.handlers.LoginHandler;
import engine.handlers.PopularKeywordHandler;
import engine.handlers.RegisterHandler;
import engine.handlers.SearchHandler;
import engine.handlers.SpellcheckHandler;
import engine.service.ConfigService;
import engine.service.FeatureService;
import engine.service.KeywordCountService;
import engine.service.PerformanceService;
import engine.service.UserService;
import engine.utils.ServerAddress;
import io.github.cdimascio.dotenv.Dotenv;
import storage.MySQLConfig;
import storage.MySQLStorage;

import static spark.Spark.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;


public class SearchEngine {
    final static Logger logger = LogManager.getLogger(SearchEngine.class);
    static List<String> indexerAddresses = new ArrayList<>();
    static List<String> rankerAddresses = new ArrayList<>();
    static EngineBdb bdb;
    static MySQLStorage db;
    
    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("engine", Level.INFO);
        Dotenv dotenv = Dotenv.configure().load();

        // init mysql connection
        MySQLConfig config = new MySQLConfig(
            dotenv.get("ENGINE_MYSQL_DBNAME"), 
            dotenv.get("ENGINE_MYSQL_USERNAME"), 
            dotenv.get("ENGINE_MYSQL_PASSWORD"),
            dotenv.get("ENGINE_MYSQL_HOSTNAME"), 
            dotenv.get("ENGINE_MYSQL_PORT"));
        db = new MySQLStorage(config);

        try {
            UserService.init(db);
        } catch (SQLException e1) {
            logger.error("Cannot init user service");
            return;
        }
        FeatureService.init(db);
        KeywordCountService.init(db);
        PerformanceService.init(db);
        ConfigService.init(db);

        int port = Integer.valueOf(dotenv.get("ENGINE_PORT"));
        port(port);
        threadPool(16);

        // set up frontend build
        // Path path = Paths.get("").toAbsolutePath();
        // Path buildFolder = Paths.get(path.getParent().toAbsolutePath().toString(), "frontend", "build");
        // System.out.println(buildFolder.toString());
        // staticFiles.externalLocation(buildFolder.toString());

        if (!Files.exists(Paths.get("enginebdb"))) {
            try {
                Files.createDirectory(Paths.get("enginebdb"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        bdb = new EngineBdb("enginebdb");
        EngineBdbViews views = new EngineBdbViews(bdb);
        Runnable rankCacheCollector = new Runnable() {
            @Override
            public void run() {
                // send every 30 seconds
                while (true) {
                    try {
                        // send dummy query to remote server
                        views.clearCache();
                        // sleep 30 s
                        Thread.sleep(ConfigService.getConfig("rank_cache_ttl"));
                    } catch (InterruptedException | SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        Thread rankCacheCollectorT = new Thread(rankCacheCollector);
        rankCacheCollectorT.start();

        before((req, resp) -> {
            logger.info(req.requestMethod() + " " + req.pathInfo());
        });

        before(new TokenFilter());

        before("/api/features", new AdminFilter());
        before("/api/shutdown", new AdminFilter());
        before("/api/configs", new AdminFilter());

        // search api
        get("/api/search", new SearchHandler(ServerAddress.parse(dotenv.get("INDEXER_SERVER")), ServerAddress.parse(dotenv.get("RANKER_SERVER")), views));

        // user api
        get("/api/login", new LoginHandler());
        get("/api/register", new RegisterHandler());

        // feature api
        get("/api/features", (req, res) -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(FeatureService.getFeatures());
        });

        post("/api/features", (req, res) -> {
            ObjectMapper mapper = new ObjectMapper();
            Feature feat = mapper.readValue(req.body(), Feature.class);
            FeatureService.addFeature(feat.getName(), feat.getCoeff(), feat.isUseLog());
            return "";
        });

        put("/api/features", (req, res) -> {
            ObjectMapper mapper = new ObjectMapper();
            Feature feat = mapper.readValue(req.body(), Feature.class);
            FeatureService.updateFeature(feat.getName(), feat.getCoeff(), feat.isUseLog());
            return "";
        });

        // spellcheck api
        get("/api/spellcheck", new SpellcheckHandler());

        // popuplar keywords api
        get("/api/trending", new PopularKeywordHandler());

        // shutdown api
        get("/api/shutdown", (req, res) -> {
            db.close();
            bdb.close();
            stop();
            return "shutdown successfully";
        });

        // config api
        get("/api/configs", (req, res) -> {
            Map<String, Integer> configs = ConfigService.getConfigs();
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(configs);
        });
        put("/api/configs/:name", (req, res) -> {
            String name = req.params("name");
            int newValue = Integer.valueOf(req.body());
            ConfigService.updateConfig(name, newValue);
            return "";
        });

        // frontend
        // get("/*", (req, res) -> {
        //     res.redirect("/");
        //     return "";
        // });

        System.out.println("Search Engine started on port: " + port);
        awaitInitialization();
    }
}
