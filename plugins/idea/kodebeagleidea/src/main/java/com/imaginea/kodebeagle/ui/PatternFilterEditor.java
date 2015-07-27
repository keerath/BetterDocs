/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.imaginea.kodebeagle.ui;

import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.project.Project;
import com.intellij.ui.AnActionButton;
import com.intellij.ui.AnActionButtonRunnable;
import com.intellij.ui.TableUtil;
import com.intellij.ui.ToolbarDecorator;
import com.intellij.ui.UIBundle;
import com.intellij.ui.table.JBTable;
import com.intellij.util.IconUtil;
import com.intellij.util.containers.ContainerUtil;
import com.intellij.util.ui.ComponentWithEmptyText;
import com.intellij.util.ui.ItemRemovable;
import com.intellij.util.ui.StatusText;
import com.intellij.util.ui.UIUtil;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PatternFilterEditor extends JPanel implements ComponentWithEmptyText {
    private static final int WIDTH = 200;
    private static final int PREFERRED_SCROLLABLE_VIEWPORT_HEIGHT_IN_ROWS = 7;
    private JBTable myTable = null;
    private FilterTableModel myTableModel = null;
    private final Project myProject;
    @Nullable
    private final String myPatternsHelpId;


    public PatternFilterEditor(final Project project) {
        this(project, null);
    }

    public PatternFilterEditor(final Project project, final String patternsHelpId) {
        super(new BorderLayout());
        myPatternsHelpId = patternsHelpId;
        myTable = new JBTable();
        myProject = project;
        final ToolbarDecorator decorator = ToolbarDecorator.createDecorator(myTable);
        decorator.addExtraAction(new AnActionButton(getAddPatternButtonText(),
                getAddPatternButtonIcon()) {
            @Override
            public void actionPerformed(final AnActionEvent e) {
                addPatternFilter();
            }
        });

        add(decorator.setRemoveAction(new AnActionButtonRunnable() {
            @Override
            public void run(final AnActionButton button) {
                TableUtil.removeSelectedItems(myTable);
            }
        }).setButtonComparator(getAddPatternButtonText(), "Remove")
                .disableUpDownActions().createPanel(), BorderLayout.CENTER);

        myTableModel = new FilterTableModel();
        myTable.setModel(myTableModel);
        myTable.setShowGrid(false);
        myTable.setShowGrid(false);
        myTable.setIntercellSpacing(new Dimension(0, 0));
        myTable.setTableHeader(null);
        myTable.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
        myTable.setColumnSelectionAllowed(false);
        myTable.setPreferredScrollableViewportSize(new Dimension(WIDTH,
                myTable.getRowHeight() * PREFERRED_SCROLLABLE_VIEWPORT_HEIGHT_IN_ROWS));

        TableColumnModel columnModel = myTable.getColumnModel();
        TableColumn column = columnModel.getColumn(FilterTableModel.CHECK_MARK);
        TableUtil.setupCheckboxColumn(column);
        column.setCellRenderer(new EnabledCellRenderer(myTable.getDefaultRenderer(Boolean.class)));
        columnModel.getColumn(FilterTableModel.FILTER).setCellRenderer(new FilterCellRenderer());

        getEmptyText().setText(UIBundle.message("no.patterns"));
    }

    @NotNull
    @Override
    public final StatusText getEmptyText() {
        return myTable.getEmptyText();
    }

    protected final String getAddPatternButtonText() {
        return UIBundle.message("button.add.pattern");
    }

    protected final Icon getAddPatternButtonIcon() {
        return IconUtil.getAddPatternIcon();
    }

    public final void setFilters(final com.intellij.ui.classFilter.ClassFilter[] filters) {
        myTableModel.setFilters(filters);
    }

    public final com.intellij.ui.classFilter.ClassFilter[] getFilters() {
        return myTableModel.getFilters();
    }

    public final void setEnabled(final boolean enabled) {
        super.setEnabled(enabled);
        myTable.setEnabled(enabled);
        myTable.setRowSelectionAllowed(enabled);
        myTableModel.fireTableDataChanged();
    }

    public final void stopEditing() {
        TableCellEditor editor = myTable.getCellEditor();
        if (editor != null) {
            editor.stopCellEditing();
        }
    }

    protected final class FilterTableModel extends AbstractTableModel implements ItemRemovable {
        private final List<com.intellij.ui.classFilter.ClassFilter> myFilters = new LinkedList<>();
        public static final int CHECK_MARK = 0;
        public static final int FILTER = 1;

        public void setFilters(final com.intellij.ui.classFilter.ClassFilter[] filters) {
            myFilters.clear();
            if (filters != null) {
                ContainerUtil.addAll(myFilters, filters);
            }
            fireTableDataChanged();
        }

        public com.intellij.ui.classFilter.ClassFilter[] getFilters() {
            for (Iterator<com.intellij.ui.classFilter.ClassFilter> it = myFilters.iterator();
                 it.hasNext();) {
                com.intellij.ui.classFilter.ClassFilter filter = it.next();
                String pattern = filter.getPattern();
                if (pattern == null || "".equals(pattern)) {
                    it.remove();
                }
            }
            return myFilters.toArray(new com.intellij.ui.classFilter.ClassFilter
                    [myFilters.size()]);
        }

        public void addRow(final com.intellij.ui.classFilter.ClassFilter filter) {
            myFilters.add(filter);
            int row = myFilters.size() - 1;
            fireTableRowsInserted(row, row);
        }

        public int getRowCount() {
            return myFilters.size();
        }

        public int getColumnCount() {
            return 2;
        }

        public Object getValueAt(final int rowIndex, final int columnIndex) {
            com.intellij.ui.classFilter.ClassFilter filter = myFilters.get(rowIndex);
            if (columnIndex == FILTER) {
                return filter;
            }
            if (columnIndex == CHECK_MARK) {
                return filter.isEnabled();
            }
            return null;
        }

        public void setValueAt(final Object aValue, final int rowIndex,
                               final int columnIndex) {
            com.intellij.ui.classFilter.ClassFilter filter = myFilters.get(rowIndex);
            if (columnIndex == FILTER) {
                if (aValue != null) {
                    filter.setPattern(aValue.toString());
                } else {
                    filter.setPattern("");
                }
            } else if (columnIndex == CHECK_MARK) {
                filter.setEnabled(aValue == null || (Boolean) aValue);
            }
            fireTableRowsUpdated(rowIndex, rowIndex);
        }

        public Class getColumnClass(final int columnIndex) {
            if (columnIndex == CHECK_MARK) {
                return Boolean.class;
            }
            return super.getColumnClass(columnIndex);
        }

        public boolean isCellEditable(final int rowIndex, final int columnIndex) {
            return isEnabled();
        }

        public void removeRow(final int idx) {
            myFilters.remove(idx);
            fireTableRowsDeleted(idx, idx);
        }
    }

    private class FilterCellRenderer extends DefaultTableCellRenderer {

        public Component getTableCellRendererComponent(final JTable table,
                                                       final Object value,
                                                       final boolean isSelected,
                                                       final boolean hasFocus,
                                                       final int row, final int column) {
            Color color = UIUtil.getTableFocusCellBackground();
            UIManager.put(UIUtil.TABLE_FOCUS_CELL_BACKGROUND_PROPERTY,
                    table.getSelectionBackground());
            Component component = super.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
            if (component instanceof JLabel) {
                ((JLabel) component).setBorder(noFocusBorder);
            }
            UIManager.put(UIUtil.TABLE_FOCUS_CELL_BACKGROUND_PROPERTY, color);
            com.intellij.ui.classFilter.ClassFilter filter =
                    (com.intellij.ui.classFilter.ClassFilter) table.getValueAt(row,
                            FilterTableModel.FILTER);
            component.setEnabled(isSelected || (PatternFilterEditor.this.isEnabled()
                    && filter.isEnabled()));
            return component;
        }
    }

    private class EnabledCellRenderer extends DefaultTableCellRenderer {
        private final TableCellRenderer myDelegate;

        public EnabledCellRenderer(final TableCellRenderer delegate) {
            myDelegate = delegate;
        }


        public Component getTableCellRendererComponent(final JTable table,
                                                       final Object value, final boolean isSelected,
                                                       final boolean hasFocus,
                                                       final int row, final int column) {
            Component component = myDelegate.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
            component.setEnabled(PatternFilterEditor.this.isEnabled());
            return component;
        }
    }


    protected final com.intellij.ui.classFilter.ClassFilter createFilter(final String pattern) {
        return new com.intellij.ui.classFilter.ClassFilter(pattern);
    }

    protected final void addPatternFilter() {
        PatternFilterEditorAddDialog dialog = new PatternFilterEditorAddDialog(
                myProject, myPatternsHelpId);
        if (dialog.showAndGet()) {
            String pattern = dialog.getPattern();
            if (pattern != null && !pattern.trim().isEmpty()) {
                com.intellij.ui.classFilter.ClassFilter filter = createFilter(pattern);
                myTableModel.addRow(filter);
                int row = myTableModel.getRowCount() - 1;
                myTable.getSelectionModel().setSelectionInterval(row, row);
                myTable.scrollRectToVisible(myTable.getCellRect(row, 0, true));
                myTable.requestFocus();
            }
        }
    }
}
