package nl.biopet.tools.digenicsearch

import nl.biopet.utils.tool.ToolCommand

object DigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    //TODO: Execute code

    logger.info("Done")
  }

  def descriptionText: String =
    """
      |
    """.stripMargin

  def manualText: String =
    s"""
       |
     """.stripMargin

  def exampleText: String =
    """
      |
    """.stripMargin
}
