package no.ntnu.stodist.debugLogger;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * The DebugLogger class allows for a quick, easy and toggelable debug print interface.
 */
public class DebugLogger {

    private final boolean print;

    /**
     * Makes a debug logger
     *
     * @param print whether or not the logger should print
     */
    public DebugLogger(boolean print) {
        this.print = print;
    }


    /**
     * Prints ping and the usual call location from the stack. used for debugging progression
     */
    public void log() {
        if (this.print) {
            this.log("<ping>");
        }
    }

    /**
     * prints arbitrary list of objects using the objects toString method
     * the location where the print statement where printed from is also shown.
     *
     * @param tolog the objects to log
     */
    public void log(Object... tolog) {
        if (this.print) {
            String printString = Stream.of(tolog).map(Objects::toString).collect(Collectors.joining(" "));

            System.out.printf("%-70s\t\t", printString);
            System.out.println(this.getFormattedCallerStackPosString());
        }
    }

    /**
     * prints arbitrary list of objects using the objects toString method
     * this is s(imple)Log that is without the stack location of the print
     *
     * @param tolog the objects to log
     */
    public void sLog(Object... tolog) {
        if (this.print) {
            String printString = Stream.of(tolog).map(Objects::toString).collect(Collectors.joining(" "));
            System.out.println(printString);
        }
    }


    public void dumpStackHere() {
        if (this.print) {
            System.out.printf("##### dumping stack at: %s #####\n", this.getFormattedCallerStackPosString());
            for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
                System.out.println(stackTraceElement);
            }
            System.out.println("##### dumping stack end #####");
        }

    }


    /**
     * A more specilized loggging for files where the file name, parent, type and so on is shown.
     *
     * @param file the file to debug
     */
    public void fileLog(File file) {
        try {
            System.out.printf("-- Debug file log at %s --\n", this.getFormattedCallerStackPosString());
            System.out.printf(
                    "File name      : %s\n" +
                            "File parent    : %s\n" +
                            "Caonical path  : %s\n", file.getName(), file.getParent(), file.getCanonicalPath());
            if (! file.exists()) {
                System.out.println("- file dont exist -");
            } else if (file.isDirectory()) {
                System.out.printf(
                        "- file is directory -\n" +
                                "Num children    : %s\n", file.listFiles().length);
            } else if (file.isFile()) {
                System.out.printf(
                        "- file is file -\n" +
                                "Size             : %s\n", file.length());
            }

            System.out.println("-- log end --");
        } catch (IOException e) {
            System.out.println("debug err IO exeption");
        }
    }

    /**
     * returns the current stack pos color formatted
     *
     * @return the current stack pos color formatted
     */
    private String getFormattedCallerStackPosString() {
        StackTraceElement tracePos = this.getCallerStackPoisson();
        if (tracePos != null) {
            return String.format("\u001B[32m%s.%s\u001B[0m(\u001B[36m%s:%d\u001B[0m)",
                                 tracePos.getClassName(),
                                 tracePos.getMethodName(),
                                 tracePos.getFileName(),
                                 tracePos.getLineNumber()
            );
        } else {
            return " ";
        }
    }


    /**
     * Returns the first stack trace element outside this class.
     * Used to find where the log methode is called from
     *
     * @return the first stack trace element outside this class.
     */
    private StackTraceElement getCallerStackPoisson() {
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            if (! stackTraceElement.getClassName().equals(this.getClass().getName())
                    && ! stackTraceElement.getMethodName().equals("getStackTrace")) {
                return stackTraceElement;
            }
        }
        return null;
    }


}
