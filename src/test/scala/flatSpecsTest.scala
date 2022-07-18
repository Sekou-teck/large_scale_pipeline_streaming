// Utilisation de la suite flatSpecs


import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._

class flatSpecsTest extends AnyFlatSpec with Matchers {
  "la division" should ("renvoyer 10") in {
    assert(HelloWorldBigData.division(20, 2) === 10)
  }

  "An arithmecal error" should("be thrown") in {
   an [ArithmeticException] should be  thrownBy(HelloWorldBigData.division(20, 0))
  }

  it should("send an OutfBound Error") in {
    var liste_fruits : List[String] = List("Banane", "Pamplemousse", "Goyave")
    assertThrows[IndexOutOfBoundsException](liste_fruits(4)) // je remplace assetThrows par l'erreur trouvée, donc à la place de (ArithmeticException) je mets (IndexOutOfBoundsException), ça passe
  }

  it should("return the starting letters of the string") in {
    var chaine : String = "chaine de caractère"
    chaine should startWith("c")
  }

}
