from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length

class SubmissionForm(FlaskForm):
    title = StringField('Title', 
                           validators=[DataRequired(), Length(min=2, max=30)])
    content = StringField('Description')
    tags = StringField('Tags',
                       validators=[DataRequired()])
    submit = SubmitField('Submit')
