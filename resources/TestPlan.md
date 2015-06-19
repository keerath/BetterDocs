#### 1. [idea] Show initial message centered.
1. When plugin opened initially help message in all pane should be at center.
<img src='screenshots/centered_help_info.png'/> <br>
<img src='screenshots/es_no_results_info.png'/> <br>
<img src='screenshots/only_java.png'/> <br>

#### 2. [idea] Introduced async fetching of results from elastic search server.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/131'> #131 </a>
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/127'> #127 </a>
1. While fetching results IDE should not hang.

#### 3. [ideaplugin] show help info should autmatically bring the focus to main pane.
##### Display the content centered.
1. When help message is displayed all pane should get focus and help content should be in center.

#### 4. [ideaplugin] #141 Plugin throwing AIOBE has been fixed.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/141'> #141 </a>
1. Add this line 'import ' in your code and hit refresh buttion.
2. NullPointerException will not be thrown.

#### 5. [ideaplugin] fixed the white background if highlighted code in new tab in main editor  is selected
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/117'> #117 </a>
1. When results in all pane/featured pane opened as new tab in IDE, selecting highlighted code 
	should be visible.
<img src='screenshots/117_1.png'/> <br>
<img src='screenshots/117_2.png'/> <br>

#### 6. [ideaplugin] added git icon to tree nodes and goto github right click menu item
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/129'> #129 </a>
1. All Pane project tree non leaf nodes should have git icon.
2. All Pane project tree nodes right click "Go to Github" should have git icon.
<img src='screenshots/129_1.png'/> <br>
<img src='screenshots/129_2.png'/> <br>

#### 7. [ideaplugin]fixed editor window scroll
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/124'> #124 </a>
1. All Pane preview editor should be scrollable.<br>
2. <img src='screenshots/124.png'/>

#### 8. [ideaplugin] removed foldings from main pane editor
##### modified wrong esURL message
##### disposed tiny editors in code pane after usage
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/112'> #112 </a>
1. All Pane preview editor should not have code foldings
2. No 'Editor has not been released..' Exception after closing the plugin.<br>
<img src='screenshots/109_2.png'/> 

#### 9. [ideaplugin] Right click and open files in a tab. + Expand to full source in a tab.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/56'> #56 </a>
##### made files in editor readonly
##### added for jtree selection update preview pane, rightclick open in new tab
1. Right clicking project tree leaf nodes should have menu item as "Open in new tab" and clicking that should open file in new tab in IDE.
2. Opened file should be non editable.
3. Selecting leaf node in project tree should load it's preview in all pane preview editor.<br>
<img src='screenshots/109_1.png'/> <br>
<img src='screenshots/109_3.png'/> <br>
<img src='screenshots/109_2.png'/> <br>

#### 10. [ideaplugin] Ignored case while searching for import names in the selected text region.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/114'> #114 </a>
1. Covered in ExtractImportsInLinesSuite test.<br>
<img src='screenshots/114_1.png'/>
<img src='screenshots/114_2.png'/>

#### 11. [ideaplugin] Registered window editor with disposer to release after it's usage.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/67'> #67 </a>
1. No 'Editor has not been released..' Exception after closing the plugin in idea.log/console.<br>
<img src='screenshots/107.png'/> 

#### 12. [ideaplugin] Taking number of tiny editors in featured pane through settings panel.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/174'> #174 </a>
1. Click on settings in toolbar. This should open Settings panel.<br>
<img src='screenshots/174.png'/>
2. Change Featured Count to 10 and click on 'Apply' button followed by 'Ok' button
3. Hit refresh button.
4. In Featured Pane atmost 10 tiny editors should get populated.

#### 13. [ideaplugin] Tabbed UI alterations.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/150'> #150 </a>
1. Open the Kodebeagle plugin, it should have two panes named Featured and all from left to right.<br>
<img src='screenshots/150.png'/>
2. Hit refresh button to check search results in both panes.

#### 14. [ideaplugin] Plugin throwing exception if HTTP response code from elastic search is not 200.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/139'> #139 </a>
1. Open Settings Panel, change 'Elastic Search URL' to labs.imaginea.com.<br>
<img src='screenshots/139.png'/>
2. Apply and click 'Ok'.
3. Hit refresh button.
4. All Pane Should have message "Connection Error: 404 Not Found".<br>
<img src='screenshots/connection_error.png'/>
5. No Exception after closing the plugin in idea.log/console.

#### 15. [ideaplugin] Rename "Repo Stars" in Code Pane to "Score:" and make "Repo Name" clickable.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/151'> #151 </a>
1. Open Kodebeagle plugin, select code and hit refresh action.
2. Check the featured pane tiny editors header, clicking on the project name should open project repo in github.com.<br>
<img src='screenshots/151.png'/>

#### 16. [ideaplugin] Expire notification bubble in 3 or 4 seconds.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/157'> #157 </a>
1. Open Kodebeagle plugin, select code and hit refresh action.
2. If ES Result returns no results notification should expire.<br>
<img src='screenshots/157.png'/>

#### 17. [ideaplugin] NPE while using the plugin.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/135'> #135 </a>
1. Open Kodebeagle plugin, select code from any non java file and hit refresh action.
2. No NullPointerException after closing the plugin in idea.log/console.

#### 18. [ideaplugin] Add UUID to request context and settins panel.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/168'> #168 </a>
1. Open Kodebeagle plugin, click on settings and check the generated plugin ID.<br>
<img src='screenshots/168.png'/>
2. Plugin ID added to request context as well.

#### 19. [ideaplugin] Add plugin and idea version to user-agent.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/194'> #194 </a>
1. Intellij idea version and plugin version added to user-agent.

#### 20. [ideaplugin] TinyEditors are not honoring soft wrap of intellij.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/126'> #126 </a>
1. Open Kodebeagle plugin, select code and hit refresh action.
2. Make sure soft wrap is checked in intellij editor settings.<br>
<img src='screenshots/soft_wrap.png'/>
3. Check the featured pane tiny editors, try to resizing them.Tiny editors should honor intellij soft wrap.<br>
<img src='screenshots/126.png'/>

#### 21. [idea] #191 Added icon to plugin tool window in plugin.xml instead of setting it in MainWindow
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/191'> #191 </a>
1. KodeBeagle idea plugin should have icon even before you invoke it.<br>
<img src='screenshots/toolWindowIcon.png'/>

#### 22. [idea] #153 Added license agreement for using kodebeagle for first time
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/153'> #153 </a>
1. Using KodeBeagle for the first time should produce a Legal Notice dialog.
2. It should have two options "Accept" and "Decline"
3. On clicking "Accept" that dialog should disappear and allows you to use kode beagle.
4. On clicking "Decline" IDE should restart and kodebeagle is disabled.<br>
<img src='screenshots/legalNotice.png'/>

#### 23. [idea] #180 clearing all pane preview editor before every new request
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/180'> #180 </a>
1. After every new request(clicking refresh action button) contents of main window preview editor should be cleared.<br>
<img src='screenshots/clearMainPanePreviewEditor.png'/>

#### 24. [idea] #83 added keyboard shortcut to plugin
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/83'> #83 </a>
1. KodeBeagle idea plugin can be shown/hidden by hitting "alt" + 8

#### 25. [idea] #50 results will be updated on opening plugin window 
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/50'> #50 </a>
1. Every time plugin is opened(if previously it's hidden) results get automatically refreshed.

#### 26. [idea] #184 Fixed functionality of reset button in Settings Panel,
#### 'Reset' and 'Apply' buttons appear when values are changed.
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/184'> #184 </a>
1. Open settings panel from main window and type some values and click 'Ok' or 'Apply'.<br>
<img src = 'screenshots/initially-applying-settings.png'/>
2. Reopen the panel and all the values that were persisted or previously applied should be displayed.<br>
<img src = 'screenshots/reopening-to-check-settings.png'/>
3. Also when there is a change in any of the values from previous values, the 'Apply' and 'Reset' 
button should appear.<br>
<img src = 'screenshots/modified-settings.png'/>
4. Open settings panel and make some changes in the values without clicking on 'Apply'.<br>
<img src = 'screenshots/before-reset.png'/>
5. Click on 'Reset' Button which should reset all the values to previously set values.<br>
<img src = 'screenshots/after-reset.png'/>

#### 27. [idea] #200 Show correct help message when no imports are present in java file
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/200> #200 </a>
1. Open KodeBeagle plugin, select code from any java file (with no imports present) and hit refresh action.
2. All pane should have message "Got nothing to search".<br>
<img src='screenshots/150.png'/>

#### 28. [idea] #203 Validation of User Input in Settings Panel 
##### fixes <a href='https://github.com/Imaginea/KodeBeagle/issues/203'> #203 </a>
1. Open Settings Panel from Main Window.
2. Type illegal values such as empty values, values with special characters in the distance, result size, featured
count. It should indicate respective warning messages.<br>
<img src = 'screenshots/multiple-validations.png'/>
3. Type a deformed URL in the Elastic Search URL box. It should indicate saying that its not a well formed 
URL.<br>
<img src = 'screenshots/url-deformed.png'/><br>
4.Enter values more than 200, 500, 15 for distance, result size and featured count respectively. It should
indicate saying that values more than these limits are not allowed.<br>
<img src = 'screenshots/value-limit.png'/><br>
5.Try entering more than 9 digits in distance, result size and featured count and it will not allow you to.<br>
<img src = 'screenshots/digit-limit.png'/><br>
6.Try clicking on 'Apply' button when there are validation errors. The apply button will not fade indicating that
these values can't be applied.<br>

