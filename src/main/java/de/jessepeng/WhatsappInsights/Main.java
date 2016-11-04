package de.jessepeng.WhatsappInsights;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextValueInputFormat;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Created by Jan-Christopher on 19.10.2016.
 */
public class Main {

    private static final Pattern NACHRICHT_PATTERN = Pattern.compile("(.*?): (.*?): (.*?)", Pattern.DOTALL);
    private static final Pattern NACHRICHT_PATTERN_ONE_LINE = Pattern.compile("^(.*?): (.*?): (.*?)$");

    private static final String BILD_NACHRICHT = "<Bild weggelassen>";
    private static final String AUDIO_NACHRICHT = "<Audio weggelassen>";
    private static final String VIDEO_NACHRICHT = "<Video weggelassen>";

    private static final String COMMAND_HAEUFIGKEIT = "haeufigkeit";
    private static final String COMMAND_WORT = "wort";
    private static final String COMMAND_WORT_NUTZER = "wortNutzer";
    private static final String COMMAND_WORT_NUTZER_DATUM = "wortNutzerDatum";
    public static final String COMMAND_WORT_ANZAHL_DURCHSCHNITT = "wortAnzahlDurchschnitt";
    public static final String DELIM = " \t\n\r\f.,:;-!?\"'()“”";
    public static final String COMMAND_NACHRICHTEN_ABSTAND = "nachrichtenAbstand";
    public static final String COMMAND_NACHRICHTEN_PRO_TAG = "nachrichtenProTag";
    public static final String COMMAND_WORT_DURCHSCHNITT_STUNDE = "wortDurchschnittStunde";
    public static final String COMMAND_WORT_DURCHSCHNITT_TAG = "wortDurchschnittTag";
    public static final String COMMAND_WORT_DURCHSCHNITT_SENDER = "wortDurchschnittSender";

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String datei = parameter.getRequired("eingabe");
        String ausgabe = parameter.getRequired("ausgabe");
        String command = parameter.getRequired("befehl");

        Map<String, String> nummerMap = new HashMap<>();
        nummerMap.put("bombig", "Benjamin Tschierschke");
        nummerMap.put("\u202A+90 553 157 0708\u202C", "Vicky Tettling");
        nummerMap.put("\u202A+49 176 55384961\u202C", "Vicky Tettling");
        nummerMap.put("\u202A+49 176 56726229\u202C", "Ronja Wiesenthal");
        nummerMap.put("\u202A+49 1516 4966879\u202C", "Nico Puchinger");
        nummerMap.put("Josefine Gröhst", "Josephine Gröhst");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().enableObjectReuse();

        DataSet<Tuple4<Date, String, String, NachrichtenTyp>> nachrichten = env.readFile(new WhatsappChatTextInputFormat(new Path(datei)), datei).map(s -> {
            Matcher nachrichtMatcher = NACHRICHT_PATTERN.matcher(s);
            if (nachrichtMatcher.matches()) {
                Date datum = new SimpleDateFormat("dd.MM.yy, hh:mm:ss").parse(nachrichtMatcher.group(1));
                String von = nachrichtMatcher.group(2);
                von = nummerMap.getOrDefault(von, von);
                String nachricht = nachrichtMatcher.group(3);
                NachrichtenTyp nachrichtenTyp;
                switch (nachricht) {
                    case BILD_NACHRICHT:
                        nachrichtenTyp = NachrichtenTyp.BILD;
                        break;
                    case AUDIO_NACHRICHT:
                        nachrichtenTyp = NachrichtenTyp.SPRACHE;
                        break;
                    case VIDEO_NACHRICHT:
                        nachrichtenTyp = NachrichtenTyp.VIDEO;
                        break;
                    default:
                        nachrichtenTyp = NachrichtenTyp.TEXT;
                        break;
                }
                return new Tuple4<Date, String, String, NachrichtenTyp>(datum, von, nachricht, nachrichtenTyp);
            }
            return null;
        });

        DataSet<?> resultDataset;
        label:
        switch (command) {
            case COMMAND_HAEUFIGKEIT:
                resultDataset = nachrichten.map((tuple) -> new Tuple2<>(tuple.f1, 1)).groupBy(0).sum(1);
                break;
            case COMMAND_WORT:
            case COMMAND_WORT_NUTZER:
            case COMMAND_WORT_NUTZER_DATUM:
                String wort = parameter.get("wort");
                DataSet<Tuple3<String, String, Date>> alleWoerter = nachrichten.flatMap((tuple, out) -> {
                    if (tuple.f3 == NachrichtenTyp.TEXT) {
                        StringTokenizer tokenizer = new StringTokenizer(tuple.f2, DELIM);
                        while (tokenizer.hasMoreTokens()) {
                            out.collect(new Tuple3<String, String, Date>(tokenizer.nextToken().toLowerCase(), tuple.f1, tuple.f0));
                        }
                    }
                });
                switch (command) {
                    case COMMAND_WORT:
                        DataSet<Tuple2<String, Integer>> anzahlWoerter = alleWoerter.map((tuple) -> new Tuple2<>(tuple.f0, 1)).groupBy(0).sum(1);
                        if (wort == null) {
                            resultDataset = anzahlWoerter;
                        } else {
                            Pattern wortPattern = Pattern.compile(wort);
                            resultDataset = anzahlWoerter.filter((tuple) -> wortPattern.matcher(tuple.f0).matches());
                        }
                        break label;
                    case COMMAND_WORT_NUTZER:
                        DataSet<Tuple3<String, String, Integer>> woerterProNutzer = alleWoerter.map((tuple) -> new Tuple3<String, String, Integer>(tuple.f0, tuple.f1, 1));
                        Pattern wortPattern = Pattern.compile(wort);
                        resultDataset = woerterProNutzer.groupBy(0, 1).sum(2).filter((tuple) -> wortPattern.matcher(tuple.f0).matches());
                        break;
                    default:
                        return;
                }
                break;
            case COMMAND_WORT_ANZAHL_DURCHSCHNITT:
                DataSet<Tuple2<String, List<String>>> wortListe = nachrichten.flatMap((tuple, out) -> {
                    if (tuple.f3 == NachrichtenTyp.TEXT) {
                        List<String> woerter = new ArrayList<String>();
                        StringTokenizer tokenizer = new StringTokenizer(tuple.f2, DELIM);
                        while (tokenizer.hasMoreTokens()) {
                            woerter.add(tokenizer.nextToken());
                        }
                        out.collect(new Tuple2<String, List<String>>(tuple.f1, woerter));
                    }
                });
                DataSet<Tuple3<String, Integer, Integer>> nachrichtenLaenge = wortListe.map((tuple) -> new Tuple3<String, Integer, Integer>(tuple.f0, tuple.f1.size(), 1));
                resultDataset = nachrichtenLaenge
                        .groupBy(0)
                        .reduce((tuple1, tuple2) -> new Tuple3<String, Integer, Integer>(tuple1.f0, tuple1.f1 + tuple2.f1, tuple1.f2 + tuple2.f2))
                        .map(tuple -> new Tuple2<String, Double>(tuple.f0, new BigDecimal(((double) tuple.f1 / tuple.f2)).round(new MathContext(4)).doubleValue()));
                break;
            case COMMAND_NACHRICHTEN_ABSTAND:
                DataSet<Tuple3<String, Long, Long>> nachrichtenAbstaende = nachrichten.groupBy(1).sortGroup(0, Order.ASCENDING).reduceGroup((values, out) -> {
                    Tuple4<Date, String, String, NachrichtenTyp> previousTuple = null;
                    for (Tuple4<Date, String, String, NachrichtenTyp> tuple: values) {
                        if (previousTuple != null) {
                            LocalDateTime previousTime = LocalDateTime.ofInstant(previousTuple.f0.toInstant(), ZoneId.systemDefault());
                            LocalDateTime currentTime = LocalDateTime.ofInstant(tuple.f0.toInstant(), ZoneId.systemDefault());
                            Duration duration = Duration.between(previousTime, currentTime);
                            out.collect(new Tuple3<String, Long, Long>(tuple.f1, duration.getSeconds(), 1L));
                        }
                        previousTuple = tuple;
                    }
                });

                resultDataset = nachrichtenAbstaende.groupBy(0).reduce((tuple1, tuple2) -> new Tuple3<String, Long, Long>(
                        tuple1.f0,
                        tuple1.f1 + tuple2.f1,
                        tuple1.f2 + tuple2.f2))
                .map(tuple -> new Tuple2<String, Double>(tuple.f0, new BigDecimal(((double) tuple.f1 / tuple.f2 / 60)).round(new MathContext(4)).doubleValue()));
                break;
            case COMMAND_NACHRICHTEN_PRO_TAG:
                resultDataset = nachrichten.map(tuple -> {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMYYYY");
                    return new Tuple2<String, Integer>(dateFormat.format(tuple.f0), 1);
                }).groupBy(0).sum(1).sortPartition(1, Order.DESCENDING);
                break;
            case COMMAND_WORT_DURCHSCHNITT_STUNDE:
                int numberOfDays = Integer.valueOf(parameter.getRequired("tage"));
                resultDataset = nachrichten
                    .map(tuple -> {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMYYYYHH");
                        return new Tuple3<String, Date, Integer>(dateFormat.format(tuple.f0), tuple.f0, 1);
                    })
                    .groupBy(0)
                    .sum(2)
                    .map(tuple -> {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("HH");
                        return new Tuple2<String, Integer>(dateFormat.format(tuple.f1), tuple.f2);
                    })
                    .groupBy(0)
                    .reduce((tuple1, tuple2) -> new Tuple2<String, Integer>(tuple1.f0, tuple1.f1 + tuple2.f1))
                    .map(tuple -> new Tuple2<String, Double>(tuple.f0, new BigDecimal(((double) tuple.f1 / numberOfDays)).round(new MathContext(4)).doubleValue()));
                break;
            case COMMAND_WORT_DURCHSCHNITT_TAG:
                resultDataset = nachrichten
                        .map(tuple -> {
                            DateFormat dateFormat = new SimpleDateFormat("ddMMYYYY");
                            return new Tuple3<String, Date, Integer>(dateFormat.format(tuple.f0), tuple.f0, 1);
                        })
                        .groupBy(0)
                        .sum(2)
                        .map(tuple -> {
                            Calendar calendar = new GregorianCalendar();
                            calendar.setTime(tuple.f1);
                            return new Tuple3<String, Integer, Integer>(String.valueOf(calendar.get(Calendar.DAY_OF_WEEK)), tuple.f2, 1);
                        })
                        .groupBy(0)
                        .reduce((tuple1, tuple2) -> new Tuple3<String, Integer, Integer>(tuple1.f0, tuple1.f1 + tuple2.f1, tuple1.f2 + tuple2.f2))
                        .map(tuple -> {
                            return new Tuple2<String, Double>(tuple.f0, new BigDecimal(((double) tuple.f1 / tuple.f2)).round(new MathContext(4)).doubleValue());
                        });
                break;
            case COMMAND_WORT_DURCHSCHNITT_SENDER:
                numberOfDays = Integer.valueOf(parameter.getRequired("tage"));
                resultDataset = nachrichten
                        .map(tuple -> {
                            DateFormat dateFormat = new SimpleDateFormat("ddMMYYYY");
                            return new Tuple4<String, String, Date, Integer>(tuple.f1, dateFormat.format(tuple.f0), tuple.f0, 1);
                        })
                        .groupBy(0, 1)
                        .sum(3)
                        .map(tuple -> new Tuple2<String, Integer>(tuple.f0, tuple.f3))
                        .groupBy(0)
                        .sum(1)
                        .map(tuple -> {
                            return new Tuple2<String, Double>(tuple.f0, new BigDecimal(((double) tuple.f1 / numberOfDays)).round(new MathContext(4)).doubleValue());
                        });
                break;
            default:
                System.out.println("Befehl nicht erkannt.");
                System.exit(-1);
                return;
        }

        env.setParallelism(1);
        if (parameter.get("format", "txt").equalsIgnoreCase("csv")) {
            resultDataset.writeAsCsv(ausgabe, FileSystem.WriteMode.OVERWRITE);
        } else {
            resultDataset.writeAsText(ausgabe, FileSystem.WriteMode.OVERWRITE);
        }

        env.execute();
    }

    private static class WhatsappChatTextInputFormat extends TextValueInputFormat {

        public WhatsappChatTextInputFormat(Path filePath) {
            super(filePath);
        }

        final StringValue emptyStringValue = new StringValue();

        @Override
        public StringValue nextRecord(StringValue record) throws IOException {
            StringValue nextLine = super.nextRecord(new StringValue());
            if (record.equals(emptyStringValue)) {
                if (nextLine == null) {
                    return null;
                } else {
                    StringValue returnValue = nextRecord(nextLine);
                    record.setValue(nextLine);
                    return returnValue;
                }
            } else {
                if (nextLine == null) {
                    return record;
                }
                Matcher nextLineMatcher = NACHRICHT_PATTERN_ONE_LINE.matcher(nextLine.getValue());
                if (nextLineMatcher.matches()) {
                    StringValue returnValue = new StringValue(record);
                    record.setValue(nextLine);
                    return returnValue;
                } else {
                    return nextRecord((StringValue)record.append(System.lineSeparator()).append(nextLine));
                }
            }
        }
    }
}
