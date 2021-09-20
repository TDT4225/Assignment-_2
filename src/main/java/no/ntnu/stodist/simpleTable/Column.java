package no.ntnu.stodist.simpleTable;

import java.util.ArrayList;
import java.util.function.Function;

/**
 * A column in the simpleTable.
 * The colums is constructing the string cells displayed in the {@link SimpleTable}
 *
 * @param <T> the type of the object to display values form
 */
public class Column<T> {
    private String header;
    private Function<T, ?> valueConstructor;

    private int cellSize;
    private boolean fitCellSize;

    /**
     * Constructs a column with the given valueConstructor and column header
     *
     * @param header           the column header for the column
     * @param valueConstructor A function returning the value to display from the Column object
     */
    public Column(String header, Function<T, ?> valueConstructor) {
        this.setHeader(header);
        this.valueConstructor = valueConstructor;

        this.cellSize    = 0;
        this.fitCellSize = true;
    }

    /**
     * Sets the column header.
     *
     * @param newHeader the new column header.
     */
    public void setHeader(String newHeader) {
        if (newHeader == null) {
            this.header = " ";
        } else {
            this.header = newHeader;
        }
    }

    /**
     * Manually sets the column width (characters).
     * Disables the default autoFit with.
     *
     * @param cellSize the width (characters) to set the column.
     */
    public void setCellSize(int cellSize) {
        this.cellSize    = cellSize;
        this.fitCellSize = false;
    }


    /**
     * Returns the value constructed from the given element, formatted in to a string cell.
     *
     * @param element the object to construct a cell value from.
     *
     * @return the constructed value formatted to a string cell.
     */
    String getFormattedCell(T element) {
        String cell = String.format(getFormatString(), valueConstructor.apply(element));
        return cell;
    }

    /**
     * Fits the cell size to the given objects constructed value.
     * if the header width is wider then the constructed value nothing is changed
     *
     * @param elements the collection of elements to get the biggest cell size from
     */
    void fitCellSize(ArrayList<T> elements) {
        if (fitCellSize) {
            cellSize = header.length();
            elements.stream().parallel().forEach(element -> {
                String testValue = String.valueOf(valueConstructor.apply(element));
                if (cellSize < testValue.length()) {
                    cellSize = testValue.length();
                }
            });
        }
    }

    /**
     * Returns a cell with the column header formatted to the column width.
     *
     * @return the formatted cell with the header
     */
    public String getFormattedHeader() {
        String cell = String.format(getFormatString(), header);
        return cell;
    }


    /**
     * Makes the template cell string to format the value's inn to.
     *
     * @return the template format string.
     */
    private String getFormatString() {
        String formatStr = "%-";
        if (cellSize == 0) {
            cellSize = header.length();
        }

        formatStr += cellSize + "s";

        formatStr += "|";
        return formatStr;
    }
}
