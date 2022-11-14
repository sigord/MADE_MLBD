import breeze.linalg._
import breeze.stats._
import breeze.numerics._
import breeze.plot._

import java.io.File


object Path{
  val train: String = "../train.csv"
  val test: String = "../test.csv"
  val pictures: String = "../pictures/"
}

object LossAndDerivatives {

  def mse(X:DenseMatrix[Double], Y:DenseVector[Double], W:DenseVector[Double]) : Double = {
    val v = X * W - Y
    mean(v * v)
  }

  def mae(X: DenseMatrix[Double], Y: DenseVector[Double], W:DenseVector[Double]): Double = {
    mean(abs(X * W - Y))
  }

  def l1_reg(W:DenseVector[Double]) : Double = {
    sum(abs(W))
  }

  def l2_reg(W:DenseVector[Double]) : Double = {
    sum(W * W)
  }

  def no_reg(W:DenseVector[Double]) : Double = {
    0.0
  }

  def mse_dir(X: DenseMatrix[Double], Y: DenseVector[Double], W:DenseVector[Double]): DenseVector[Double] = {
    (X.t * (X * W - Y)) *:* 2.0 /:/ convert(Y.length, Double)
  }

  def mae_dir(X: DenseMatrix[Double], Y: DenseVector[Double], W:DenseVector[Double]): DenseVector[Double] = {
    (X.t * signum((X * W) - Y)) /:/ convert(Y.length, Double)
  }

  def l1_reg_dir(W:DenseVector[Double]) : DenseVector[Double] = {
    signum(W)
  }

  def l2_reg_dir(W:DenseVector[Double]) : DenseVector[Double] = {
    W *:* 2.0
  }

  def no_reg_dir(W:DenseVector[Double]) :DenseVector[Double] = {
    DenseVector.zeros[Double](W.length)
  }

  def lr_loss_setup(loss: String): ((DenseMatrix[Double], DenseVector[Double], DenseVector[Double]) => Double, (DenseMatrix[Double], DenseVector[Double], DenseVector[Double]) => DenseVector[Double]) = {
    loss match {
      case "mse" =>
        val loss_function = LossAndDerivatives.mse _
        val loss_derivative = LossAndDerivatives.mse_dir _
        (loss_function, loss_derivative)

      case "mae" =>
        val loss_function = LossAndDerivatives.mae _
        val loss_derivative = LossAndDerivatives.mae_dir _
        (loss_function, loss_derivative)
    }
  }

  def lr_reg_setup(reg_mode: String): (DenseVector[Double] => Double, DenseVector[Double] => DenseVector[Double]) = {
    reg_mode match {
      case "l1" =>
        val reg_function = LossAndDerivatives.l1_reg _
        val reg_derivative = LossAndDerivatives.l1_reg_dir _
        (reg_function, reg_derivative)

      case "l2" =>
        val reg_function = LossAndDerivatives.l2_reg _
        val reg_derivative = LossAndDerivatives.l2_reg_dir _
        (reg_function, reg_derivative)

      case "no_reg" =>
        val reg_function = LossAndDerivatives.no_reg _
        val reg_derivative = LossAndDerivatives.no_reg_dir _
        (reg_function, reg_derivative)
    }
  }
}

object Score {
  def R2(y_true: DenseVector[Double], y_pred: DenseVector[Double]): Double = {
    val ssr = sum((y_true - y_pred) * (y_true - y_pred))
    val mean_true = mean(y_true)
    val sst = sum((y_true - mean_true) * (y_true - mean_true))
    1 - ssr/sst
  }
}

class LinearRegression(loss: String,
                       reg_mode: String,
                       lr : Double = 0.05,
                       n_steps : Int = 22000,
                       reg_coeff : Double = 0.05,
                       normX : Boolean = true){
  var W: DenseVector[Double] = DenseVector()
  val(reg_function, reg_derivative) = LossAndDerivatives.lr_reg_setup(reg_mode)
  val(loss_fun, loss_derivative) = LossAndDerivatives.lr_loss_setup(loss)


  def fit(X_val: DenseMatrix[Double], Y: DenseVector[Double]): (Array[Double], Array[Double]) = {
    var X: DenseMatrix[Double] = X_val.copy
    var score: Double = 0.0
    var loss_history = Array[Double]()
    var score_history = Array[Double]()
    this.W = DenseVector.ones[Double](X.cols)
    if (normX) {
      X = X(::, *).map(Column => Column / norm(Column))
    }
    //fit
    for (i <- 1 to n_steps) {
      var empirical_risk = loss_fun(X, Y, this.W) + reg_coeff * reg_function(this.W)
      var gradient = loss_derivative(X, Y, this.W) + reg_coeff * reg_derivative(this.W)
      var gradient_norm = norm(gradient)
      if(gradient_norm > 1.0){
        gradient = gradient/gradient_norm * 1.0
      }
      this.W = this.W - lr * gradient
      // pred
      val y_pred = X.toDenseMatrix * this.W
      val y_true = Y.toDenseVector
      score = Score.R2(y_true, y_pred)
      if (i % 10 == 0) {
//        println(f"Step=$i%d, loss=$empirical_risk%f, R2=$score%f gradient=$gradient")
        loss_history = loss_history :+ empirical_risk
        score_history = score_history :+ score
      }
    }
    (loss_history, score_history)
  }

  def pred(X_val: DenseMatrix[Double]): DenseVector[Double] = {
    var X: DenseMatrix[Double] = X_val.copy
    if (normX) {
      X = X(::, *).map(Column => Column / norm(Column))
    }
    X * this.W
  }

  def plot_result(save_path: String, X: DenseVector[Double], Y: List[DenseVector[Double]],
                 X_label: String, Y_label: String): Unit ={
    val f = Figure()
    val p = f.subplot(0)
    Y.foreach{
      y_elem => p += plot(X,y_elem)
    }
    p.xlabel = X_label
    p.ylabel = Y_label
    f.saveas(Path.pictures + save_path, dpi = 100)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val train = csvread(new File(Path.train), separator = ',', skipLines = 1)
    val test = csvread(new File(Path.test), separator = ',', skipLines = 1)
    val train_X = train(::, 0 to -2)
    val train_Y = train(::, -1)
    val test_X = test(::, 0 to -2)
    val test_Y = test(::, -1)
    val LR = new LinearRegression(loss = "mse", reg_mode = "l1")
    val(loss_hist, score_hist) = LR.fit(train_X, train_Y)
    val test_pred = LR.pred(test_X)
    println(Score.R2(test_Y, test_pred))
    //plot
    var x_plot_var : DenseVector[Double] = DenseVector()

    x_plot_var = convert(DenseVector((1 to loss_hist.length).toArray), Double)
    LR.plot_result(save_path = "loss_history.png", x_plot_var, List(DenseVector(loss_hist)), "Epoch","Loss")

    LR.plot_result(save_path = "score_history.png", x_plot_var, List(DenseVector(score_hist)), "Epoch","Score")

    x_plot_var = convert(DenseVector((1 to test_Y.length).toArray), Double)
    LR.plot_result(save_path = "true_pred.png", x_plot_var, List(test_Y, test_pred), "Index","Y")
  }
}