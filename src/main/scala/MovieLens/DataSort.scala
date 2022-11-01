package MovieLens

object DataSort {

  def tokenize(line: String): List[String] = {
    line.split("\\s").toList
  }
}
