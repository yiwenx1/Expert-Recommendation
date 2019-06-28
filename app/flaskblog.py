from flask import Flask, render_template, url_for, flash, redirect
from forms import SubmissionForm
# from flask.ext.cqlalchemy import CQLAlchemy
from flask_cqlalchemy import CQLAlchemy
import uuid

app = Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'

"""
app.config['CASSANDRA_HOSTS'] = ['10.0.0.12']
app.config['CASSANDRA_KEYSPACE'] = "cqlengine"
db = CQLAlchemy(app)
"""
"""
class Score(db.Model):
    tag = db.columns.Text()
    user_id = db.columns.UUID(default=uuid.uuid4)
    all_score = db.columns.Integer()
"""

experts = [
    {
        'user_id': 1,
        'tags': 'spark',
        'upvotes': 200,
        'reputation': 500
    },
    {
        'user_id': 2,
        'tags': 'java',
        'upvotes': 190,
        'reputation': 400
    },
    {
        'user_id': 3,
        'tags': 'java, spark',
        'upvotes': 180,
        'reputation': 400
    }
        ]

@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html')

@app.route("/new_question", methods=['GET', 'POST'])
def ask_question():
    form = SubmissionForm()
    if form.validate_on_submit():
        flash('question has been submitted successfully!')
        return redirect('/experts')
    return render_template('new_question.html', title='New Question', form=form)

@app.route("/experts_recommendation")
def rec_experts():
    return render_template('experts_list.html', title='Experts', experts=experts)

@app.route("/top_experts")
def top_experts():
    return render_template('top_experts.html', title='Top Experts', experts=experts)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)
