package com.imaginea.kodebeagle.tasks;

import com.imaginea.kodebeagle.util.Utils;
import com.intellij.openapi.progress.PerformInBackgroundOption;
import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.project.Project;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class CachingTask extends Task.Backgroundable {

    private static final String KODEBEAGLE = "KodeBeagle";
    private final Utils utils = Utils.getInstance();
    private final Map<String, String> fileNameVsContent;

    public CachingTask(final Project project, final Map<String, String> pFileNameVsContent) {
        super(project, KODEBEAGLE, true, PerformInBackgroundOption.ALWAYS_BACKGROUND);
        fileNameVsContent = pFileNameVsContent;
    }

    @Override
    public final void run(@NotNull final ProgressIndicator progressIndicator) {
        for (Map.Entry<String, String> entry : fileNameVsContent.entrySet()) {
            utils.createFileWithContents(entry.getKey(),
                    entry.getValue().replaceAll("\r", ""));
        }
    }
}
