package signbook.processor;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.*;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONArray;

public class SignBookProcessor {

    private static final Logger logger = Logger.getLogger(SignBookProcessor.class.getName());
    private Properties config;
    private String inputEncoding;
    private long pollingInterval;
    private MongoCollection<Document> documents_meta_collection;
    private MongoCollection<Document> pages_collection;

    public SignBookProcessor() throws IOException {
        loadConfig();
        setupLogger();
//        connectToMongo();
    }

    private void loadConfig() throws IOException {
        config = new Properties();
        config.load(new FileInputStream("config/config.properties"));

        config.getProperty("input.dir");
        config.getProperty("output.dir");
        config.getProperty("processed.dir");
        config.getProperty("error.dir");
        config.getProperty("temp.extension", ".tmp");
        config.getProperty("final.extension", ".txt");
        config.getProperty("char.to.insert", "1");
        inputEncoding = config.getProperty("input.encoding", "ISO-8859-1");
        config.getProperty("output.encoding", "UTF-8");
        pollingInterval = Long.parseLong(config.getProperty("polling.interval", "10000"));

        String filePatternString = config.getProperty("input.file.pattern", ".*");  // Cargar la expresión regular
        Pattern.compile(filePatternString);  // Compilar la expresión regular

        String mongoUrl = System.getenv("DB_URL");
        if (mongoUrl == null) {
            mongoUrl = config.getProperty("mongo.uri", "mongodb://localhost:27017/librub");
        }
        ConnectionString connectionString = new ConnectionString(mongoUrl);
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(config.getProperty("mongo.db.name", "librub"));
        documents_meta_collection = database.getCollection("documents_meta");
        pages_collection = database.getCollection("pages");
    }

    private void setupLogger() throws IOException {
        String logFile = config.getProperty("log.file", "signBook.preprocessor.log");
        String logLevel = config.getProperty("log.level", "INFO");
        int logRotationHours = Integer.parseInt(config.getProperty("log.rotation.hours", "24"));

        FileHandler handler = new FileHandler(logFile, logRotationHours * 3600 * 1000, 1, true);
        handler.setFormatter(new SimpleFormatter());

        switch (logLevel.toUpperCase()) {
            case "DEBUG":
                logger.setLevel(Level.FINE);
                break;
            case "INFO":
                logger.setLevel(Level.INFO);
                break;
            case "ERROR":
                logger.setLevel(Level.SEVERE);
                break;
            default:
                logger.setLevel(Level.INFO);
        }

        logger.addHandler(handler);
    }

    public void start() {
        Timer timer = new Timer();
        timer.schedule(new FileProcesorTask(), 0, pollingInterval);
    }

    private class FileProcesorTask extends TimerTask {

        @Override
        public void run() {
            Bson filter = Filters.eq("status", "to process");
            FindIterable<Document> filesToProcess = documents_meta_collection.find(filter);
            if (filesToProcess.cursor().hasNext()) {
                for (Document doc : filesToProcess) {
                    logger.log(Level.INFO, "Procesando archivo {0}", doc.get("filename"));
                    updateDMStatus(doc.getObjectId("_id"), "in process");
                    try {
                        processFile(
                                Paths.get(config.getProperty("input.dir"), doc.get("filename").toString()),
                                doc.getObjectId("_id"),
                                doc.getString("page_break"));
                        updateDMStatus(doc.getObjectId("_id"), "Finished Ok");
                        logger.log(Level.INFO, "Finalizando archivos {0}", doc.get("filename"));
                    } catch (IOException ex) {
                        updateDMStatus(doc.getObjectId("_id"), "Error");
                        logger.log(Level.SEVERE, "error:", ex);
                    }
                }
            } else {
                logger.log(Level.INFO, "sin archivos para procesar");
            }
        }
    }

    private void updateDMStatus(ObjectId documentID, String status) {
        documents_meta_collection.updateOne(
                Filters.eq("_id", documentID),
                Updates.set("status", status)
        );
    }

    private void updateDMActivity(ObjectId documentID, Document activity) {
        documents_meta_collection.updateOne(
                Filters.eq("_id", documentID),
                Updates.push("activity", activity)
        );
    }

    private void processFile(Path inputFilePath, ObjectId documentId, String page_breack) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        String startTime = LocalDateTime.now().toString();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFilePath.toFile()), Charset.forName(inputEncoding))
        );) {
            String line;
            int pageNum = 0;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                if (line.equals(page_breack) && !lines.isEmpty()) {
                    insertPage(lines, documentId, pageNum);
                    pageNum++;
                    lines.clear();
                    continue;
                }
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                lines.add(line);
            }
            insertPage(lines, documentId, pageNum);
            documents_meta_collection.updateOne(
                    Filters.eq("_id", documentId),
                    Updates.set("ocurrence_publication", pageNum + 1));
            String endTime = LocalDateTime.now().toString();
            updateDMActivity(documentId, new Document()
                    .append("action", "Publication")
                    .append("start_time", startTime)
                    .append("end_time", endTime));
        }
    }

    void insertPage(ArrayList lines, ObjectId documentId, int pageNum) {
        Document page = new Document()
                .append("lines", (new JSONArray(lines.toArray())))
                .append("documentId", documentId)
                .append("number", pageNum)
                .append("createdAt", new Date());
        pages_collection.insertOne(page);
    }

    public static void main(String[] args) {
        try {
            SignBookProcessor app = new SignBookProcessor();
            app.start();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error initializing application", e);
        }
    }
}
