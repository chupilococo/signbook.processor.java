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
import java.util.List;
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
    private MongoCollection<Document> books_collection;
    private MongoCollection<Document> documents_meta_collection;
    private MongoCollection<Document> pages_collection;

    public SignBookProcessor() throws IOException {
        logger.info("Iniciando SignBookProcessor...");
        loadConfig();
        setupLogger();
        logger.info("Configuración y logger inicializados correctamente.");
    }

    private void loadConfig() throws IOException {
        logger.info("Cargando configuración desde config.properties...");
        config = new Properties();
        config.load(new FileInputStream("config/config.properties"));

        config.getProperty("input.dir");
        config.getProperty("processed.dir");  // 🔹 Se usa processedDir en lugar de output.dir
        config.getProperty("error.dir");
        config.getProperty("temp.extension", ".tmp");
        config.getProperty("final.extension", ".txt");
        config.getProperty("char.to.insert", "1");
        inputEncoding = config.getProperty("input.encoding", "ISO-8859-1");
        config.getProperty("output.encoding", "UTF-8");
        pollingInterval = Long.parseLong(config.getProperty("polling.interval", "10000"));

        String filePatternString = config.getProperty("input.file.pattern", ".*");
        Pattern.compile(filePatternString);

        String mongoUrl = System.getenv("DB_URL");
        if (mongoUrl == null) {
            mongoUrl = config.getProperty("mongo.uri");
        }
        logger.log(Level.INFO, "Conectando a MongoDB en: {0}", mongoUrl);
        ConnectionString connectionString = new ConnectionString(mongoUrl);
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(config.getProperty("mongo.db.name"));
        documents_meta_collection = database.getCollection("documents_meta");
        pages_collection = database.getCollection("pages");
        books_collection = database.getCollection("books");
        logger.info("Conexión a MongoDB establecida correctamente.");
    }

    private void setupLogger() throws IOException {
        String logFile = config.getProperty("log.file", "signBook.processor.log");
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
        logger.log(Level.INFO, "Iniciando tarea programada para procesamiento de archivos cada {0}ms...", pollingInterval);
        Timer timer = new Timer();
        timer.schedule(new FileProcessorTask(), 0, pollingInterval);
    }

    void insertPage(ArrayList<String> lines, ObjectId documentId, int pageNum, Document indexes) {
        Document page = new Document()
                .append("lines", new JSONArray(lines.toArray()))
                .append("documentId", documentId)
                .append("number", pageNum)
                .append("createdAt", new Date())
                .append("indexes", indexes); // 🔹 Agregar los índices extraídos

        pages_collection.insertOne(page);
    }

    private class FileProcessorTask extends TimerTask {

        @Override
        public void run() {
            logger.info("Buscando archivos en estado 'to process' en sistema...");
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
                        updateDMStatus(doc.getObjectId("_id"), "finished Ok");
                        logger.log(Level.INFO, "Finalizando archivo {0}", doc.get("filename"));
                    } catch (IOException ex) {
                        updateDMStatus(doc.getObjectId("_id"), "error");
                        logger.log(Level.SEVERE, "Error al procesar el archivo:", ex);
                    }
                }
            } else {
                logger.log(Level.INFO, "No hay archivos para procesar.");
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

    private void processFile(Path inputFilePath, ObjectId documentId, String page_break) throws IOException {
        logger.log(Level.INFO, "Iniciando procesamiento del archivo: {0}", inputFilePath.getFileName());
        ArrayList<String> lines = new ArrayList<>();
        String startTime = LocalDateTime.now().toString();
        int pageNum = 0;
        boolean firstLine = true;

        // 🔹 Obtener `bookId` desde `documents_meta`
        Document docMeta = documents_meta_collection.find(Filters.eq("_id", documentId)).first();
        if (docMeta == null || !docMeta.containsKey("book")) {
            logger.severe("No se encontró el documento en documents_meta o no tiene asociado un bookId.");
            return;
        }
        ObjectId bookId = docMeta.getObjectId("book");

        // 🔹 Obtener `indexes` desde `books`
        Document book = books_collection.find(Filters.eq("_id", bookId)).first();
        Document indexConfig = (book != null) ? (Document) book.get("indexes") : new Document();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFilePath.toFile()), Charset.forName(inputEncoding))
        )) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals(page_break) && !lines.isEmpty()) {
                    logger.log(Level.FINE, "P\u00e1gina {0} detectada. Insertando en MongoDB...", pageNum);

                    // 🔹 Extraer índices de la página usando la configuración de `books`
                    Document extractedIndexes = extractIndexes(lines, indexConfig);

                    insertPage(lines, documentId, pageNum, extractedIndexes);
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

            // Procesar última página
            Document extractedIndexes = extractIndexes(lines, indexConfig);
            insertPage(lines, documentId, pageNum, extractedIndexes);

            logger.log(Level.INFO, "\u00daltima p\u00e1gina {0} insertada en sistema.", pageNum);

            documents_meta_collection.updateOne(
                    Filters.eq("_id", documentId),
                    Updates.set("ocurrence_publication", pageNum + 1));
            String endTime = LocalDateTime.now().toString();
            updateDMActivity(documentId, new Document()
                    .append("action", "Publication")
                    .append("start_time", startTime)
                    .append("end_time", endTime));
            logger.log(Level.INFO, "Procesamiento finalizado para archivo: {0}", inputFilePath.getFileName());

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error procesando archivo: " + inputFilePath.getFileName(), e);
            throw e;
        }

        moveToProcessedDir(inputFilePath);
    }

    private Document extractIndexes(List<String> lines, Document indexConfig) {
        Document extractedIndexes = new Document();
        boolean extractIndex = true;
        for (String indexKey : indexConfig.keySet()) {
            Document configuration = (Document) indexConfig.get(indexKey);

            if (configuration == null) {
                continue;
            }

            // ✅ Verificar que cada campo existe antes de accederlo
            Document lineRange = configuration.get("line_range", Document.class);
            Document position = configuration.get("position", Document.class);
            String patternStr = configuration.getString("pattern");
            String endPatternStr = configuration.getString("end_pattern");

            if (lineRange == null || position == null || patternStr == null) {
                continue;
            }

            Integer startLine = lineRange.getInteger("start");
            Integer endLine = lineRange.getInteger("end");
            Integer startPos = position.getInteger("start");
            Integer endPos = position.getInteger("end");

            if (startLine == null || endLine == null || startPos == null || endPos == null || patternStr.isEmpty()) {
                continue;
            }

            Pattern pattern = Pattern.compile(patternStr);
            Pattern endPattern = null;
            if (endPatternStr != null) {
                endPattern = Pattern.compile(endPatternStr);
            }
            List<String> indexValues = new ArrayList<>();

            // Recorrer las líneas dentro del rango definido en MongoDB
            for (int i = startLine; i <= endLine && i < lines.size(); i++) {
                String line = lines.get(i).trim(); // Quitar espacios antes y después

                if (endPattern != null) {
                    extractIndex = !endPattern.matcher(line).matches();
                }

                // Asegurar que la línea tiene suficiente longitud
                if (line.length() >= endPos) {
                    String extractedValue = line.substring(startPos, endPos).trim(); // Extraer y limpiar espacios

                    // Validar con regex
                    if (!extractIndex && pattern.matcher(extractedValue).matches() && !indexValues.contains(extractedValue)) {
                        indexValues.add(extractedValue); // Agregar solo valores únicos
                    }
                }
            }

            extractedIndexes.append(indexKey, indexValues);
        }

        return extractedIndexes;
    }

    private void moveToProcessedDir(Path inputFilePath) {
        try {
            Path processedFilePath = Paths.get(config.getProperty("processed.dir"), inputFilePath.getFileName().toString());
            Files.move(inputFilePath, processedFilePath, StandardCopyOption.REPLACE_EXISTING);
            logger.log(Level.INFO, "\u2705 Archivo movido a la carpeta de procesados: {0}", processedFilePath);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "❌ Error al mover el archivo a la carpeta de procesados: " + inputFilePath.getFileName(), e);
        }
    }

    public static void main(String[] args) {
        logger.info("Iniciando aplicación SignBookProcessor...");
        try {
            SignBookProcessor app = new SignBookProcessor();
            app.start();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error al iniciar la aplicación", e);
        }
    }
}
