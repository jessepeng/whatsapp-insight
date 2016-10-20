package de.jessepeng.WhatsappInsights;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextValueInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
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

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String datei = parameter.getRequired("eingabe");
        String ausgabe = parameter.getRequired("ausgabe");
        String command = parameter.getRequired("befehl");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().enableObjectReuse();

        DataSet<Tuple4<Date, String, String, NachrichtenTyp>> nachrichten = env.readFile(new WhatsappChatTextInputFormat(new Path(datei)), datei).map(s -> {
            Matcher nachrichtMatcher = NACHRICHT_PATTERN.matcher(s);
            if (nachrichtMatcher.matches()) {
                Date datum = new SimpleDateFormat("dd.MM.yy, hh:mm:ss").parse(nachrichtMatcher.group(1));
                String von = nachrichtMatcher.group(2);
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
                        StringTokenizer tokenizer = new StringTokenizer(tuple.f2, " \t\n\r\f.,:;-!?\"'()“”");
                        while (tokenizer.hasMoreTokens()) {
                            out.collect(new Tuple3<String, String, Date>(tokenizer.nextToken().toLowerCase(), tuple.f1, tuple.f0));
                        }
                    }
                });
                DataSet<Tuple2<String, Integer>> anzahlWoerter = alleWoerter.map((tuple) -> new Tuple2<>(tuple.f0, 1)).groupBy(0).sum(1);
                if (wort == null) {
                    resultDataset = anzahlWoerter;
                } else {
                    Pattern wortPattern = Pattern.compile(wort);
                    resultDataset = anzahlWoerter.filter((tuple) -> wortPattern.matcher(tuple.f0).matches());
                }
                break;
            default:
                System.out.println("Befehl nicht erkannt.");
                System.exit(-1);
                return;
        }

        env.setParallelism(1);
        resultDataset.writeAsText(ausgabe, FileSystem.WriteMode.OVERWRITE);

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
