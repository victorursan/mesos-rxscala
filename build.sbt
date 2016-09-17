name := "mesos-rxscala"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies := {
  val rxScala  = "0.26.2"
  val mesos    = "1.0.1"
  val netty    = "4.1.5.Final"
  val protobuf = "2.6.1"
  val junit = "4.12"
  Seq(
    "io.reactivex"        % "rxscala_2.11"  % rxScala,
    "org.apache.mesos"    % "mesos"         % mesos,
    "io.netty"            % "netty-buffer"  % netty,
    "com.google.protobuf" % "protobuf-java" % protobuf,
    "junit"               % "junit"         % junit

  )
}