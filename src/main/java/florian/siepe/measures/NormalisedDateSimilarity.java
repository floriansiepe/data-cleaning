package florian.siepe.measures;

import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class NormalisedDateSimilarity extends SimilarityMeasure<LocalDateTime> {

    private static final long serialVersionUID = 1L;
    private LocalDateTime minDate;
    private LocalDateTime maxDate;
    private int dateRange;

    public LocalDateTime getMinDate() {
        return this.minDate;
    }

    public void setMinDate(final LocalDateTime minDate) {
        this.minDate = minDate;
    }

    public LocalDateTime getMaxDate() {
        return this.maxDate;
    }

    public void setMaxDate(final LocalDateTime maxDate) {
        this.maxDate = maxDate;
    }

    public int getDateRange() {
        return this.dateRange;
    }

    public void setValueRange(final LocalDateTime minValue, final LocalDateTime maxValue) {
        minDate = minValue;
        maxDate = maxValue;
        this.calcDateRange();
    }

    private void calcDateRange() {
        if (null != minDate && null != maxDate) {
            this.dateRange = (int) Math.abs(ChronoUnit.DAYS.between(maxDate, minDate));
        }
    }

    @Override
    public double calculate(final LocalDateTime first, final LocalDateTime second) {
        final int days = (int) Math.abs(ChronoUnit.DAYS.between(first, second));

        return Math.max(1.0 - ((double) days / dateRange), 0.0);
    }


}
