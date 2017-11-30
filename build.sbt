organization := "com.github.biopet"
//TODO: change name
name := "tool-template"

scalaVersion := "2.11.11"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.2-SNAPSHOT" changing()
libraryDependencies += "com.github.biopet" %% "tool-test-utils" % "0.1-SNAPSHOT" % Test changing()

//TODO: change mainClass
mainClass in assembly := Some("nl.biopet.tools.template.ToolTemplate")

useGpg := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  releaseStepCommand("git fetch"),
  releaseStepCommand("git checkout master"),
  releaseStepCommand("git pull"),
  releaseStepCommand("git merge origin/develop"),
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  releaseStepCommand("sonatypeReleaseAll"),
  pushChanges,
  releaseStepCommand("git checkout develop"),
  releaseStepCommand("git merge master"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

// Documentation stuff
val urlToolName="tool-template"
val classPrefix="nl.biopet.tools.template"

import LaikaKeys._
enablePlugins(LaikaSitePlugin)
enablePlugins(SiteScaladocPlugin)
enablePlugins(GhpagesPlugin)
enablePlugins(PreprocessPlugin)

val docsDir: String="target/markdown/"
val readme: String="./README.md"

sourceDirectory in LaikaSite := file(docsDir)
sourceDirectories in Laika := Seq((sourceDirectory in LaikaSite).value)
rawContent in Laika := true

git.remoteRepo := s"git@github.com:biopet/$urlToolName.git"
ghpagesRepository := file("target/gh")

// Puts Scaladoc output in `in /api subfolder`
siteSubdirName in SiteScaladoc := s"${version.value}/api"
siteDirectory in Laika  := file("target/site")
excludeFilter in ghpagesCleanSite := new FileFilter{
 def accept(f: File) = true
}

lazy val generateDocs = taskKey[Unit]("Generate documentation files")
lazy val generateReadme = taskKey[Unit]("Generate readme")

generateDocs := {
  import sbt.Attributed.data
  val r = (runner in Runtime).value
  val input = Seq(docsDir, version.value, (!isSnapshot.value).toString)
  val classPath =  (fullClasspath in Runtime).value
  r.run(
    s"$classPrefix.Documentation",
    data(classPath),
    input,
    streams.value.log
  ).foreach(sys.error)
  }
generateReadme := {
  import sbt.Attributed.data
  val r: ScalaRun = (runner in Runtime).value
  val input = Seq(readme)
  val classPath =  (fullClasspath in Runtime).value
  r.run(
    s"$classPrefix.Readme",
    data(classPath),
    input,
    streams.value.log
  ).foreach(sys.error)
}
makeSite <<= makeSite.triggeredBy(generateDocs)
makeSite <<= makeSite dependsOn generateDocs
ghpagesPushSite <<= ghpagesPushSite dependsOn makeSite