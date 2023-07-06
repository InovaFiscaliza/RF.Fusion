// ! Base module copied from https://learn.microsoft.com/en-us/archive/msdn-magazine/2016/october/windows-service-create-a-customizable-filesystemwatcher-windows-service
// TODO: Include file indexing process and methods to perform task equivalent to linux version
// TODO: Check https://github.com/javiersanzsanchez/windows-filesystemwatcher

/// <summary>
/// This class defines an individual type of file and its associated
/// folder to be monitored by the File System Watcher
/// </summary>
public class CustomFolderSettings
{
  /// <summary>Unique identifier of the combination File type/folder.
  /// Arbitrary number (for instance 001, 002, and so on)</summary>
  [XmlAttribute]
  public string FolderID { get; set; }
  /// <summary>If TRUE: the file type and folder will be monitored</summary>
  [XmlElement]
  public bool FolderEnabled { get; set; }
  /// <summary>Description of the type of files and folder location –
  /// Just for documentation purpose</summary>
  [XmlElement]
  public string FolderDescription { get; set; }
  /// <summary>Filter to select the type of files to be monitored.
  /// (Examples: *.shp, *.*, Project00*.zip)</summary>
  [XmlElement]
  public string FolderFilter { get; set; }
  /// <summary>Full path to be monitored
  /// (i.e.: D:\files\projects\shapes\ )</summary>
  [XmlElement]
  public string FolderPath { get; set; }
  /// <summary>If TRUE: the folder and its subfolders will be monitored</summary>
  [XmlElement]
  public bool FolderIncludeSub { get; set; }
  /// <summary>Specifies the command or action to be executed
  /// after an event has raised</summary>
  [XmlElement]
  public string ExecutableFile { get; set; }
  /// <summary>List of arguments to be passed to the executable file</summary>
  [XmlElement]
  public string ExecutableArguments { get; set; }
  /// <summary>Default constructor of the class</summary>       
  public CustomFolderSettings()
  {
  }
}

/// <summary>Reads an XML file and populates a list of <CustomFolderSettings> </summary>
private void PopulateListFileSystemWatchers()
{
  // Get the XML file name from the App.config file
  fileNameXML = ConfigurationManager.AppSettings["XMLFileFolderSettings"];
  // Create an instance of XMLSerializer
  XmlSerializer deserializer =
    new XmlSerializer(typeof(List<CustomFolderSettings>));
  TextReader reader = new StreamReader(fileNameXML);
  object obj = deserializer.Deserialize(reader);
  // Close the TextReader object
  reader.Close();
  // Obtain a list of CustomFolderSettings from XML Input data
  listFolders = obj as List<CustomFolderSettings>;
}

/// <summary>Start the file system watcher for each of the file
/// specification and folders found on the List<>/// </summary>
private void StartFileSystemWatcher()
{
  // Creates a new instance of the list
  this.listFileSystemWatcher = new List<FileSystemWatcher>();
  // Loop the list to process each of the folder specifications found
  foreach (CustomFolderSettings customFolder in listFolders)
  {
    DirectoryInfo dir = new DirectoryInfo(customFolder.FolderPath);
    // Checks whether the folder is enabled and
    // also the directory is a valid location
    if (customFolder.FolderEnabled && dir.Exists)
    {
      // Creates a new instance of FileSystemWatcher
      FileSystemWatcher fileSWatch = new FileSystemWatcher();
      // Sets the filter
      fileSWatch.Filter = customFolder.FolderFilter;
      // Sets the folder location
      fileSWatch.Path = customFolder.FolderPath;
      // Sets the action to be executed
      StringBuilder actionToExecute = new StringBuilder(
        customFolder.ExecutableFile);
      // List of arguments
      StringBuilder actionArguments = new StringBuilder(
        customFolder.ExecutableArguments);
      // Subscribe to notify filters
      fileSWatch.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName |
        NotifyFilters.DirectoryName;
      // Associate the event that will be triggered when a new file
      // is added to the monitored folder, using a lambda expression                   
      fileSWatch.Created += (senderObj, fileSysArgs) =>
        fileSWatch_Created(senderObj, fileSysArgs,
         actionToExecute.ToString(), actionArguments.ToString());
      // Begin watching
      fileSWatch.EnableRaisingEvents = true;
      // Add the systemWatcher to the list
      listFileSystemWatcher.Add(fileSWatch);
      // Record a log entry into Windows Event Log
      CustomLogEvent(String.Format(
        "Starting to monitor files with extension ({0}) in the folder ({1})",
        fileSWatch.Filter, fileSWatch.Path));
    }
  }
}

/// <summary>This event is triggered when a file with the specified
/// extension is created on the monitored folder</summary>
/// <param name="sender">Object raising the event</param>
/// <param name="e">List of arguments - FileSystemEventArgs</param>
/// <param name="action_Exec">The action to be executed upon detecting a change in the File system</param>
/// <param name="action_Args">arguments to be passed to the executable (action)</param>
void fileSWatch_Created(object sender, FileSystemEventArgs e,
  string action_Exec, string action_Args)
{
  string fileName = e.FullPath;
  // Adds the file name to the arguments. The filename will be placed in lieu of {0}
  string newStr = string.Format(action_Args, fileName);
  // Executes the command from the DOS window
  ExecuteCommandLineProcess(action_Exec, newStr);
}

/// <summary>Executes a set of instructions through the command window</summary>
/// <param name="executableFile">Name of the executable file or program</param>
/// <param name="argumentList">List of arguments</param>
private void ExecuteCommandLineProcess(string executableFile, string argumentList)
{
  // Use ProcessStartInfo class
  ProcessStartInfo startInfo = new ProcessStartInfo();
  startInfo.CreateNoWindow = true;
  startInfo.UseShellExecute = false;
  startInfo.FileName = executableFile;
  startInfo.WindowStyle = ProcessWindowStyle.Hidden;
  startInfo.Arguments = argumentList;
try
  {
    // Start the process with the info specified
    // Call WaitForExit and then the using-statement will close
    using (Process exeProcess = Process.Start(startInfo))
    {
      exeProcess.WaitForExit();
      // Register a log of the successful operation
      CustomLogEvent(string.Format(
        "Succesful operation --> Executable: {0} --> Arguments: {1}",
        executableFile, argumentList));
    }
  }
  catch (Exception exc)
  {
    // Register a Log of the Exception
  }
}

/// <summary>Event automatically fired when the service is started by Windows</summary>
/// <param name="args">array of arguments</param>
protected override void OnStart(string[] args)
{
  // Initialize the list of FileSystemWatchers based on the XML configuration file
  PopulateListFileSystemWatchers();
  // Start the file system watcher for each of the file specification
  // and folders found on the List<>
  StartFileSystemWatcher();
}

/// <summary>Event automatically fired when the service is stopped by Windows</summary>
protected override void OnStop()
{
  if (listFileSystemWatcher != null)
  {
    foreach (FileSystemWatcher fsw in listFileSystemWatcher)
    {
      // Stop listening
      fsw.EnableRaisingEvents = false;
      // Dispose the Object
      fsw.Dispose();
    }
    // Clean the list
    listFileSystemWatcher.Clear();
  }
}