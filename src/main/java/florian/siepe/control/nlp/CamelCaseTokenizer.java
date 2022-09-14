package florian.siepe.control.nlp;

import org.deeplearning4j.text.tokenization.tokenizer.TokenPreProcess;
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;

import java.util.Arrays;
import java.util.List;

public class CamelCaseTokenizer implements Tokenizer {
    private final String input;
    private String[] tokens;
    private int iteratorIndex;
    private TokenPreProcess tokenPreProcessor;

    public CamelCaseTokenizer(String input) {
        this.input = input;
    }

    @Override
    public boolean hasMoreTokens() {
        return null == this.tokens;
    }

    @Override
    public int countTokens() {
        return this.tokens.length;
    }

    @Override
    public String nextToken() {
        if (null == this.tokens) {
            tokens = Arrays.stream(this.input.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])"))
                    .map(s -> null != tokenPreProcessor ? this.tokenPreProcessor.preProcess(s) : s)
                    .toArray(String[]::new);
        }
        if (iteratorIndex >= tokens.length) {
            return null;
        } else {
            String s = tokens[this.iteratorIndex];
            this.iteratorIndex++;
            return s;
        }
    }

    @Override
    public List<String> getTokens() {
        if (null == this.tokens) {
            nextToken();
        }
        return List.of(this.tokens);
    }

    @Override
    public void setTokenPreProcessor(TokenPreProcess tokenPreProcess) {
        tokenPreProcessor = tokenPreProcess;
    }
}
