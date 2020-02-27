
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import play.api.libs.ws.StandaloneWSClient
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}



object LogFetch {

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newFixedThreadPool(10))
  case class LogFile(val url: String, val file: String)

  val baseUrl = "http://hdp-nn-1.dev.octanner.com:50070/webhdfs/v1/user/logs"
  val folder = "applogs/prod_mile_app"
  val baseLocalFolder = "/Users/howard.fackrell/tmp"

  def main(args: Array[String]) = {

    implicit val system = ActorSystem()
    implicit val materializer: Materializer = ActorMaterializer()
    val wsClient = StandaloneAhcWSClient()

    val logFiles: Seq[LogFile] = files()

//    fetchFilesSync(logFiles, wsClient)
//    wsClient.close()
//    system.terminate()

    fetchFiles(logFiles, wsClient)
      .andThen {
        case _ => wsClient.close()
      }
      .andThen {
        case _ => system.terminate()
      }
      .andThen {
        case _ => println("System terminated")
      }

    println("Completed")
  }

  def files(): Seq[LogFile] = {
    for {
      year <- Seq(2018)
      month <- (5 to 7).toSeq
      day <- (1 to 31).toSeq
      hour <- (0 to 23).toSeq
    } yield {
      val monthFormat = "%02d".format(month)
      val dayFormat = "%02d".format(day)
      val hourFomat = "%02d".format(hour)
      val url = s"$baseUrl/$folder/$year$monthFormat$dayFormat/$hourFomat.json"
      val file = s"$folder/$year$monthFormat$dayFormat/$hourFomat.json"
      LogFile(url, file)
    }
  }

  def fetchFile(logFile: LogFile, ws: StandaloneWSClient)(implicit mat: Materializer): Future[Unit] = {
    val f: Future[Unit] = ws.url(logFile.url).addQueryStringParameters("op" -> "OPEN").withMethod("GET").stream().flatMap {
      case response =>
        println("trying...")
        if (response.status >= 200 && response.status < 400) {
          val path = Paths.get(baseLocalFolder, logFile.file)
          println(s"got path $path")
          Files.createDirectories(path.getParent)
          if (Files.exists(path)) {
            Files.delete(path)
          }
          val outputStream = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)

          val sink = Sink.foreach[ByteString] {
            bytes => outputStream.write(bytes.toArray)
          }

          response.bodyAsSource.runWith(sink).andThen {
            case _ =>
              outputStream.close()
              ()
          }.map ( _ => ())
        }
        else {
          println(s"${response.status} returned for ${logFile.url}")
          Future.successful(())
        }
    }.recoverWith {
      case t =>
        println(s"failed to get ${logFile.url} ${t.getMessage()}")
        println(t.getCause)
        t.printStackTrace()
        Future.successful(())
    }
    f
  }

  def fetchFilesSync(files: Seq[LogFile], ws: StandaloneWSClient)(implicit mat: Materializer): Unit = {
    files.foreach {
      file =>
      val f = fetchFile(file, ws)
      try {
        Await.result(f, Duration(2, TimeUnit.MINUTES))
      } catch {
        case _ => ()
      }
    }
  }

  def fetchFiles(files: Seq[LogFile], ws: StandaloneWSClient )(implicit mat: Materializer): Future[Unit] = {
    val seqOfFutures = files.map {
      file => fetchFile(file, ws)
    }

    Future.sequence(seqOfFutures).map( _ => ())
  }
}
