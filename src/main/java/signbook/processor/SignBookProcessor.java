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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.*;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.conversions.Bson;

public class SignBookProcessor {

    private static final Logger logger = Logger.getLogger(SignBookProcessor.class.getName());
    private Properties config;
    private String inputDir;
    private String outputDir;
    private String processedDir;
    private String errorDir;
    private String tempExtension;
    private String finalExtension;
    private String charToInsert;
    private String inputEncoding;
    private String outputEncoding;
    private long pollingInterval;
    private MongoCollection<Document> documents_meta_collection;
    private MongoCollection<Document> metaCollection;  // Colección para la bitácora
    private FindIterable<Document> books;
    private Pattern filePattern;  // Expresión regular para filtrar archivos

    public SignBookProcessor() throws IOException {
        loadConfig();
        setupLogger();
//        connectToMongo();
    }

    private void loadConfig() throws IOException {
        config = new Properties();
        config.load(new FileInputStream("config/config.properties"));

        inputDir = config.getProperty("input.dir");
        outputDir = config.getProperty("output.dir");
        processedDir = config.getProperty("processed.dir");
        errorDir = config.getProperty("error.dir");
        tempExtension = config.getProperty("temp.extension", ".tmp");
        finalExtension = config.getProperty("final.extension", ".txt");
        charToInsert = config.getProperty("char.to.insert", "1");
        inputEncoding = config.getProperty("input.encoding", "ISO-8859-1");
        outputEncoding = config.getProperty("output.encoding", "UTF-8");
        pollingInterval = Long.parseLong(config.getProperty("polling.interval", "10000"));

        String filePatternString = config.getProperty("input.file.pattern", ".*");  // Cargar la expresión regular
        filePattern = Pattern.compile(filePatternString);  // Compilar la expresión regular

        String mongoUrl = System.getenv("DB_URL");
        if (mongoUrl == null) {
            mongoUrl = config.getProperty("mongo.uri", "mongodb://localhost:27017/librub");
        }
        // MongoDB Configuration
        ConnectionString connectionString = new ConnectionString(mongoUrl);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(config.getProperty("mongo.db.name", "librub"));
        documents_meta_collection = database.getCollection("documents_meta");

        // Conectar a la colección "documents_meta" para registrar la bitácora
//        metaCollection = database.getCollection("documents_meta");
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
                    documents_meta_collection.updateOne(
                            Filters.eq(
                                    "_id",
                                    doc.get("_id")),
                            Updates.set(
                                    "status",
                                    "in process"));
                }
            } else {
                logger.log(Level.INFO, "sin archivos para procesar");
            }

        }
    }

    private void processFile(Path filePath) {
        Path tempFilePath = null;
        LocalDateTime startTime = LocalDateTime.now();  // Captura la hora de inicio
        String timestamp = startTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));  // Formato de timestamp
        String newFileName = timestamp + "_" + filePath.getFileName().toString();  // Nuevo nombre del archivo con timestamp
        int occurrenceCount = 0;
        String status = "to process";  // Inicializa el estado como "to process"
        String errorDescription = "";  // Inicializa la descripción de error

        try {
            tempFilePath = Paths.get(outputDir, newFileName + tempExtension);
            occurrenceCount = processAndWriteFile(filePath, tempFilePath);  // Regresa el conteo de ocurrencias

            // Mueve el archivo procesado a la carpeta de procesados con el nombre nuevo
            Files.move(filePath, Paths.get(processedDir, newFileName));
            // Mueve el archivo temporal a la carpeta de salida con el nombre nuevo y extensión final
            Files.move(tempFilePath, Paths.get(outputDir, newFileName.replace(tempExtension, finalExtension)));
            logger.log(Level.INFO, "File processed successfully: {0}", newFileName);

        } catch (Exception e) {  // Captura cualquier excepción para evitar que la aplicación se detenga
            logger.log(Level.SEVERE, "Error processing file: " + filePath.getFileName().toString(), e);
            status = "error";  // Cambia el estado a "error" si ocurre un problema
            errorDescription = e.getMessage();  // Captura la descripción del error
            if (tempFilePath != null && Files.exists(tempFilePath)) {
                try {
                    Files.delete(tempFilePath);
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Error deleting temp file: " + tempFilePath.toString(), ex);
                }
            }
            try {
                // Mueve el archivo original a la carpeta de errores con el nombre nuevo
                Files.move(filePath, Paths.get(errorDir, newFileName));
            } catch (IOException ex) {
                logger.log(Level.SEVERE, "Error moving file to error directory: " + newFileName, ex);
            }
        } finally {
            // Registrar bitácora en MongoDB con el estado y descripción de error si aplica
            Document logEntry = new Document("filename", newFileName)
                    .append("start_time", startTime.toString())
                    .append("end_time", LocalDateTime.now().toString()) // Captura la hora de fin
                    .append("occurrences", occurrenceCount)
                    .append("status", status)
                    .append("error_description", errorDescription);  // Agrega la descripción del error a la bitácora si aplica
            metaCollection.insertOne(logEntry);
        }
    }

    private int processAndWriteFile(Path inputFilePath, Path outputFilePath) throws IOException {
        int occurrenceCount = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFilePath.toFile()), Charset.forName(inputEncoding))
        ); BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outputFilePath.toFile()),
                Charset.forName(outputEncoding)))) {

            String line;
            String pgSc = getPgSeparatorForFile(inputFilePath, books);
            while ((line = reader.readLine()) != null) {
                if (line.contains(pgSc)) {
                    writer.write(charToInsert);
                    writer.newLine();
                    occurrenceCount++;  // Incrementa el conteo de ocurrencias
                }
                writer.write(line);
                writer.newLine();
            }
        }

        return occurrenceCount;  // Regresa el conteo de ocurrencias
    }

    private String getPgSeparatorForFile(Path inputFilePath, FindIterable<Document> books) throws IOException {
        String pgSeparator = null;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFilePath.toFile()), Charset.forName(inputEncoding))
        )) {
            String line;
            List<String> firstLines = new ArrayList<>();

            while ((line = reader.readLine()) != null && firstLines.size() < 5) {
                firstLines.add(line);
            }

            for (Document book : books) {
                for (String firstLine : firstLines) {
                    if (firstLine.contains(book.getString("page_break"))) {
                        pgSeparator = book.getString("page_break");
                    }
                }
            }
        }
        return pgSeparator;
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
