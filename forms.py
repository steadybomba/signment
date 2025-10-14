from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired

class TrackForm(FlaskForm):
    tracking_number = StringField('Tracking Number', validators=[DataRequired()])
