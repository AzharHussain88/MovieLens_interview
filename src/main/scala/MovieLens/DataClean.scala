package MovieLens

  object DataClean {

    def cleanData(line: String): String = {
      line
        .toLowerCase
        .replaceAll("[,.]"," ")
        .replaceAll("[^a-z0-9\\s-]","")
        .replaceAll("\\s+"," ")
        .trim
    }

}
