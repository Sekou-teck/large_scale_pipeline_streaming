import junit.framework.TestCase
import org.junit._
import org.junit.Assert._

class UnitTestBigDataJUnit {

  @Test // Annotation qui indique que la fn qui suit est un test
  def testDivision() : Unit = {
      var valeur_actuelle : Double = HelloWorldBigData.division(10, 2)
      var valeur_prevue : Int = 5

      assertEquals("résultat attendu : la fonction doit renvoyer normalement 5", valeur_prevue, valeur_actuelle.toInt)

  }

  @Test
  def testConversion () : Unit = {
    var valeur_actuelle : Int = HelloWorldBigData.convert_entier("15")
    var valeur_attendue : Int = 15
    assertSame("résultat attendu : la fonction doit normalement renvoyer un nombre", valeur_attendue, valeur_actuelle)
  }

  @Test
  def test_Conversion () : Unit = {
    var valeur_actuelle : Int = HelloWorldBigData.comptage_caracteres("exemple de caractères")
    var valeur_attendue : Int = 20
    assertSame("résultat attendu : la fonction doit normalement renvoyer un nombre", valeur_attendue, valeur_actuelle)
  }

}





