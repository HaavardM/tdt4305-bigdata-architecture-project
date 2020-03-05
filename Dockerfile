FROM mozilla/sbt

COPY . .

CMD ["sbt", "run"]