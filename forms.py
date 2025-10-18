from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length, Optional, Email

class TrackForm(FlaskForm):
    tracking_number = StringField('Tracking Number', validators=[DataRequired(), Length(min=1, max=50)])
    email = StringField('Email', validators=[Optional(), Email(), Length(max=120)])
    submit = SubmitField('Track')

# In telegram_bot.py
def start_bot():
    bot.infinity_polling()
def check_bot_status():
    # Implement logic to check bot responsiveness, e.g., via Telegram API
    return True  # Placeholder
