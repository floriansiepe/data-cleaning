package florian.siepe.control.nlp;

import org.deeplearning4j.text.tokenization.tokenizer.TokenPreProcess;
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;

import java.util.Arrays;
import java.util.List;

public class CamelCaseTokenizer implements Tokenizer {
    private final String input;
    private String[] tokens;
    private int iteratorIndex = 0;
    private TokenPreProcess tokenPreProcessor = null;

    public CamelCaseTokenizer(final String input) {
        this.input = input;
    }

    @Override
    public boolean hasMoreTokens() {
        return this.tokens == null;
    }

    @Override
    public int countTokens() {
        return tokens.length;
    }

    @Override
    public String nextToken() {
        if (this.tokens == null) {
            this.tokens = Arrays.stream(input.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])"))
                    .map(s -> tokenPreProcessor != null ? tokenPreProcessor.preProcess(s) : s)
                    .toArray(String[]::new);
        }
        return iteratorIndex >= tokens.length ? null : tokens[iteratorIndex++];
    }

    @Override
    public List<String> getTokens() {
        if (this.tokens == null) {
            this.nextToken();
        }
        return List.of(tokens);
    }

    @Override
    public void setTokenPreProcessor(final TokenPreProcess tokenPreProcess) {
        this.tokenPreProcessor = tokenPreProcess;
    }
}
