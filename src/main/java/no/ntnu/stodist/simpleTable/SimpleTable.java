package no.ntnu.stodist.simpleTable;


import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Displays a table consisting of columns constructing values from a list of given objects.
 * The table has a simple and a "fancy" display mode toggleable in setPrettyPrint() the default is true.
 * <p>
 * #########  fancy mode  #########
 * ╔═════════╦══════════╦═════════╗
 * ║AAA      ║bbb       ║CCC      ║
 * ╠═════════╬══════════╬═════════╣
 * ║200      ║abc       ║200      ║
 * ╟─────────╫──────────╫─────────╢
 * ║344      ║elele     ║344      ║
 * ╚═════════╩══════════╩═════════╝
 * <p>
 * ########  simple mode  ########
 * AAA       BBB        CCC
 * 200       abc        200
 * 33        ab2c       33
 * 344       elele      344
 * <p>
 * The title wil be displayed if any is set.
 * The row separators and is toggleable with setRowSeparators().
 *
 * @param <T> the type of the items in the table.
 */
public class SimpleTable<T> {
    private final ArrayList<T> tableItems;
    private final ArrayList<Column<T>> cols;

    private String title;

    private boolean prettyPrint;
    private boolean rowSeparators;
    private final HashMap<String, String> tableChars;

    private boolean showRowIndex;

    private int currentDisplayRow;

    private int topPaddingLines = 0;

    /**
     * constructing the SimpleTable
     */
    public SimpleTable() {
        this.tableItems = new ArrayList<>();
        this.cols       = new ArrayList<>();

        this.title         = null;
        this.prettyPrint   = true;
        this.rowSeparators = true;
        this.showRowIndex  = false;

        this.tableChars = new HashMap<>();

        tableChars.put("HOR", "═");
        tableChars.put("VER", "║");


        tableChars.put("TOP_L", "╔");
        tableChars.put("TOP_M", "╦");
        tableChars.put("TOP_R", "╗");

        tableChars.put("MID_L", "╠");
        tableChars.put("MID_M", "╬");
        tableChars.put("MID_R", "╣");

        tableChars.put("BOTTOM_L", "╚");
        tableChars.put("BOTTOM_M", "╩");
        tableChars.put("BOTTOM_R", "╝");

        tableChars.put("SEP_L", "╟");
        tableChars.put("SEP_M", "╫");
        tableChars.put("SEP_R", "╢");
        tableChars.put("SEP_HOR", "─");

    }

    /**
     * Sets the characters to build the table from.
     * the default key-character set is:
     * <p>
     * Horisontal and vertical lines
     * HOR      - ═
     * VER      - ║
     * <p>
     * The top decoration
     * TOP_L    - ╔
     * TOP_M    - ╦
     * TOP_R    - ╗
     * <p>
     * Decorations in the table
     * MID_L    - ╠
     * MID_M    - ╬
     * MID_R    - ╣
     * <p>
     * The bottom decorations
     * BOTTOM_L - ╚
     * BOTTOM_M - ╩
     * BOTTOM_R - ╝
     * <p>
     * The the decorations between lines if toggled
     * SEP_L    - ╟
     * SEP_M    - ╫
     * SEP_R    - ╢
     * SEP_HOR  - ─
     *
     * @param key       the key of the character to change
     * @param character the new character to set to the provided key
     *
     * @return if the key was found and the change successful
     */
    public boolean setTableChar(String key, Character character) {
        boolean suc = false;
        if (tableChars.containsKey(key)) {
            suc = true;
            tableChars.put(key, character.toString());
        }
        return suc;
    }

    /**
     * Sets the title to display above the table, if none is provided the title is not shown.
     *
     * @param title the title to display
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Sets the number of empty lines to print before displaying the table.
     *
     * @param topPaddingLines number of empty lines to print before displaying the table.
     */
    public void setTopPaddingLines(int topPaddingLines) {
        this.topPaddingLines = topPaddingLines;
    }

    /**
     * Sets whether or not to display a line between each row in the table.
     *
     * @param newState true if separator between the rows should be shown false if not
     */
    public void setUseRowSeparators(Boolean newState) {
        this.rowSeparators = newState;
    }

    /**
     * Sets whether or not to display a column with sequentially numbered rows.
     * The row indexes starts at 1.
     *
     * @param newState true if the index column is to be shown false if not
     */
    public void setShowRowIndex(boolean newState) {
        this.showRowIndex = newState;
    }

    /**
     * Sets whether or not to prettyPrint the table.
     * Pretty printing uses the tableChars to make the table.
     *
     * @param newState True if separator between the rows should be shown false if not
     */
    public void setPrettyPrint(boolean newState) {
        this.prettyPrint = newState;
    }


    /**
     * Sets the new table items. Shortcut for calling getItems.clear() and getItems.addAll(newItems)
     *
     * @param newItems the new item to set.
     */
    public void setItems(Collection<T> newItems) {
        tableItems.clear();
        tableItems.addAll(newItems);
    }

    /**
     * Sets the new table items from an iterator.
     *
     * @param newItemsItr the iterator containing the new items to add
     */
    public void setItems(Iterator<T> newItemsItr) {
        tableItems.clear();
        newItemsItr.forEachRemaining(tableItems::add);
    }

    /**
     * Adds a new item to the table items.
     *
     * @param tableItem
     */
    public void addItem(T tableItem) {
        tableItems.add(tableItem);
    }


    /**
     * returns the list of items the table wil display.
     *
     * @return the list of items the table will display
     */
    public List<T> getItems() {
        return tableItems;
    }


    /**
     * Sets the new table columns. Shortcut for calling getCols.clear() and getCols.addAll(newCols)
     *
     * @param newCols the new columns in the table.
     */
    public void setCols(Column<T>... newCols) {
        if (newCols.length > 0) {
            cols.clear();
            cols.addAll(Arrays.asList(newCols));
        }
    }

    /**
     * Returns the list of columns the table will display.
     *
     * @return the list of columns the table will display.
     */
    public List<Column<T>> getCols() {
        return cols;
    }


    /**
     * Displays a table with the set columns, sequentially from the items in items.
     * if no data has been provided an empty table is displayed.
     * if no Columns has been provided nothing is displayed.
     */
    public void display() {
        if (cols.isEmpty()) {
            return;
        }
        if (! tableItems.isEmpty()) {
            cols.stream().parallel().forEach(c -> c.fitCellSize(tableItems));
        }

        for (int i = 0; i < this.topPaddingLines; i++) {
            System.out.println();
        }

        this.currentDisplayRow = 0;

        String headerRow  = this.makeRow(cols.stream().map(Column::getFormattedHeader).collect(Collectors.joining()));
        int    titleLengt = (title == null) ? 0 : title.length() + 4;
        int    tableXSize = Math.max(headerRow.length(), titleLengt);

        displayTableTitle(tableXSize);


        String topDecorator = headerRow.replaceAll("[^|]", tableChars.get("HOR"));
        topDecorator = topDecorator.replace("|", tableChars.get("TOP_M"));
        topDecorator = tableChars.get("TOP_L") + topDecorator.substring(1, topDecorator.length() - 1) + tableChars.get(
                "TOP_R");


        String middleDecorator = topDecorator
                .replace(tableChars.get("TOP_R"), tableChars.get("MID_R"))
                .replace(tableChars.get("TOP_L"), tableChars.get("MID_L"))
                .replace(tableChars.get("TOP_M"), tableChars.get("MID_M"));
        String bottomDecorator = topDecorator
                .replace(tableChars.get("TOP_R"), tableChars.get("BOTTOM_R"))
                .replace(tableChars.get("TOP_L"), tableChars.get("BOTTOM_L"))
                .replace(tableChars.get("TOP_M"), tableChars.get("BOTTOM_M"));

        String sepDecorator = topDecorator
                .replace(tableChars.get("TOP_R"), tableChars.get("SEP_R"))
                .replace(tableChars.get("TOP_L"), tableChars.get("SEP_L"))
                .replace(tableChars.get("TOP_M"), tableChars.get("SEP_M"))
                .replace(tableChars.get("HOR"), tableChars.get("SEP_HOR"));


        maybeDisplayDecorator(topDecorator);
        displayRow(headerRow);
        maybeDisplayDecorator(middleDecorator);

        boolean first = true;
        for (T e : tableItems) {
            if (first) {
                first = false;
            } else if (rowSeparators) {
                maybeDisplayDecorator(sepDecorator);
            }
            String row = this.makeRow(cols.stream().map(c -> c.getFormattedCell(e)).collect(Collectors.joining()));
            displayRow(row);
        }
        maybeDisplayDecorator(bottomDecorator);
    }

    private String makeRow(String rowData) {
        String returnStr;
        if (this.showRowIndex) {
            if (this.currentDisplayRow == 0) {
                returnStr = String.format("| # |%s", rowData);
                this.currentDisplayRow++;
            } else {
                returnStr = String.format("|%-3s|%s", this.currentDisplayRow, rowData);
                this.currentDisplayRow++;
            }
        } else {
            returnStr = "|" + rowData;
        }
        return returnStr;
    }

    /**
     * Displays the provided row.
     * formats the row if prettyPrint is true
     *
     * @param row the row of data to be displayed.
     */
    private void displayRow(String row) {
        String out = row;
        if (prettyPrint) {
            out = row.replace("|", tableChars.get("VER"));
        } else {
            out = row.replace("|", " ");
        }

        System.out.println(out);
    }

    /**
     * Displays the provided decorator string if prettyPrint is true.
     *
     * @param decorator the decorator string to display.
     */
    private void maybeDisplayDecorator(String decorator) {
        if (prettyPrint) {
            System.out.println(decorator);
        }
    }


    /**
     * Displays the table title if any is set does nothing if not.
     * Displays the title centered on the table with ta provided tableXSize
     *
     * @param tableXSize The x size of the table.
     */
    private void displayTableTitle(int tableXSize) {
        if (title != null) {
            String printTitle;
            int    rest     = tableXSize - title.length() - 2;
            int    sizeSide = Math.floorDiv(rest, 2);
            String side     = repeat("#", sizeSide);
            if (rest % 2 == 0) {
                printTitle = String.format("%s %s %s", side, title, side);
            } else {
                printTitle = String.format("%s %s %s", side + "#", title, side);
            }
            System.out.println(printTitle);
        }
    }

    /**
     * Returns a string consisting of the given pattern repeted the given amount of times
     *
     * @param pattern     the pattern to repeat
     * @param repetitions the number of times to repeat the given pattern
     *
     * @return a string consisting of the given pattern repetitions amount of times
     */
    public static String repeat(String pattern, int repetitions) {
        return new String(new char[repetitions]).replace("\0", pattern);
    }
}
